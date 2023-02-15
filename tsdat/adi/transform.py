from enum import Enum
from typing import Any, Dict, List

import xarray as xr
from xarray.core.coordinates import DataArrayCoordinates
import numpy as np
import pandas as pd

import cds3
import dsproc3 as dsproc
import trans


# We will always use the same coordinate system name for every ADI dataset conversion, since Tsdat will only allow the
#   user to define one coordinate system.
COORDINATE_SYSTEM_NAME = 'coord_sys'

# I don't think the ADI transformer cares what the datastream names are, so we are going to hardcode it for now
OUTPUT_DATASTREAM_NAME = 'output_ds'
INPUT_DATASTREAM_NAME = 'input_ds'


class ADIAtts:
    MISSING_VALUE = 'missing_value'
    LONG_NAME = 'long_name'
    STANDARD_NAME = 'standard_name'
    UNITS = 'units'
    VALID_MIN = 'valid_min'
    VALID_MAX = 'valid_max'
    FILL_VALUE = '_FillValue',
    DESCRIPTION = 'description'
    ANCILLARY_VARIABLES = 'ancillary_variables'


def transform(input_var: xr.DataArray,
              input_qc_var: xr.DataArray,
              output_var: xr.DataArray,
              output_qc_var: xr.DataArray,
              pcm_transform_parameters: Dict):
    """-----------------------------------------------------------------------------------------------------------------
    This function will use ADI libraries to transform the input_var to the shape defined for the output_var.
    The transform will also fill out the output_qc_var with the approriate qc status from the transform algorithm.

    The output_var and output_qc_var's data will be written in place.  Any variable attribute values that were added by
    adi will be copied back to the output variables.

    Parameters
    ----------
    input_var : xr.DataArray
        Input variable to be transformed.

        * Note that if range transform parameters were set on any datastreams, the xarray data must have at least
         'range' amount of extra data retrieved before and after the day being processed (if it exists).

        * Note that the input variables should have been renamed to use the name from the output dataset.  So
            we should rename input variables to match their output names BEFORE we pass them to this method.

        * Note that variable dimensions should have been renamed to match their names in the output variable.

        TODO: In ADI data structures, the variable maps back to its parent datastream Group so it can get global atts.
            I do not know if

    input_qc_var : xr.DataArray
        If there is a companion QC variable associated with the input var, then it should be passed.  Otherwise, pass
        None.

        * Note variable name and dimensions should be renamed BEFORE this method is called, same as input_var above.

    output_var : xr.DataArray
        Xarray data array for the output variable.  It should have the coordinates set to the output shape and
        placeholder values for the data.  The transform will overwrite the values.

    output_qc_var : xr.DataArray
        Xarray data array for the output qc variable.  It should have the coordinates set to the output shape and
        placeholder values for the data.   The transform will overwrite the values and set variable attributes.

    pcm_transform_parameters : Dict
        Transformation params in PCM json format.  For example:

            "coordinate_system_defaults": [
              {
                "name": "coord_sys",
                "dim": "time",
                "default_value": "AUTO"
              },
              {
                "name": "coord_sys",
                "dim": "range",
                "default_value": "TRANS_PASSTHROUGH"
              }
            ],

        * Note that width and alignment parameters are applied to the coordinate system group
        TODO: Longer term, we should support front edge/back edge, width, and/or alignment parameters for input
            datastreams in order to provide more specific bounds on the data points.  For now we won't set these and the
            transformer will use the default values, which is to assume center alignment and the front and back edge
            are the previous andsubsequent points, respectively.

    -----------------------------------------------------------------------------------------------------------------"""

    # First convert the input and output variables into ADI format
    retrieved_dataset: cds3.Group = _create_adi_retrieved_dataset(input_var, input_qc_var)
    transformed_dataset: cds3.Group = _create_adi_transformed_dataset(output_var, output_qc_var)

    # Now convert the tranform parameters into ADI format
    adi_transform_parameters = _convert_transform_params_to_adi_string(pcm_transform_parameters)

    # Now apply the coordinate system transform parameters to the coordinate system group
    if COORDINATE_SYSTEM_NAME in adi_transform_parameters:
        params = adi_transform_parameters.get(COORDINATE_SYSTEM_NAME)
        cs_group = transformed_dataset.get_groups()[0]
        cds3.parse_transform_params(cs_group, params)

    # now apply the input datastream transform parameters to the obs group
    if INPUT_DATASTREAM_NAME in adi_transform_parameters:
        params = adi_transform_parameters.get(INPUT_DATASTREAM_NAME)
        obs_group = retrieved_dataset.get_groups()[0].get_groups()[0]
        cds3.parse_transform_params(obs_group, params)

    # Now run the transform
    adi_input_var = retrieved_dataset.get_groups()[0].get_groups()[0].get_var(input_var.name)
    adi_input_qc_var = retrieved_dataset.get_groups()[0].get_groups()[0].get_var(input_qc_var.name)
    adi_output_var = transformed_dataset.get_groups()[0].get_groups()[0].get_var(output_var.name)
    adi_output_qc_var = transformed_dataset.get_groups()[0].get_groups()[0].get_var(output_qc_var.name)
    trans.transform_driver(adi_input_var, adi_input_qc_var, adi_output_var, adi_output_qc_var)

    # Now copy any changed variable attributes back to the xr out variables
    def update_attrs(xr_var: xr.DataArray, adi_var: cds3.Var):
        adi_atts: List[cds3.Att] = adi_var.get_atts()
        attrs = {att.get_name(): dsproc.get_att_value(adi_var, att.get_name(), att.get_type()) for att in adi_atts}
        for name, value in attrs.items():
            xr_var.attrs[name] = value
    update_attrs(output_var, adi_output_var)
    update_attrs(output_qc_var, adi_output_qc_var)

    # Now free up memory from created adi data structures
    _free_memory(retrieved_dataset)
    _free_memory(transformed_dataset)


def _convert_transform_params_to_adi_string(pcm_transform_parameters: Dict) -> Dict[str, str]:
    # TODO: port over PCM code: ProcessRepository.php::writeOldTransformParameters
    return {}


def _free_memory(adi_dataset: cds3.Group):
    # First we MUST walk through the object tree and detatch data pointers for all variables. We need
    #   to do this because the group delete will delete everything in the hierarchy, and we don't want  to
    #   delete the data because it's being shared with xarray.
    #   * Note that we don't attach/detatch for time because we have to copy those values because of numpy datetime64
    def detatch_vars(group: cds3.Group):
        subgroups = group.get_groups()
        for subgroup in subgroups:
            detatch_vars(subgroup)

        vars: List[cds3.Var] = group.get_vars()
        for var in vars:
            if var.get_name() != 'time':
                dsproc.detatch_var_data(var)

    detatch_vars(adi_dataset)

    #  After all the variable data has been detached, then we can delete the group.
    cds3.Group.delete(adi_dataset)


def _create_adi_retrieved_dataset(xr_input_var: xr.DataArray, xr_input_qc_var: xr.DataArray) -> cds3.Group:
    """-----------------------------------------------------------------------------------------------------------------
    Create the following structure in ADI:

        Dataset Group: retrieved_data
            Datastream Group: nsametC1.b1
                Obs Group: nsametC1.b1.20140101.000000.cdf <-- dims go here
                   cds3.core.Var <-- all vars go here including coordinate vars

    Parameters
    ----------
    xr_input_var
    xr_input_qc_var

    Returns
    -------
    retrieved_dataset : cds3.Group

    -----------------------------------------------------------------------------------------------------------------"""
    # Note:  We are not initializing datastream objects (_DSProc->datastreams) because I don't think we need it for
    # any of the libtrans operations

    # First create the dataset group
    dataset_group = cds3.Group.define(None, 'retrieved_data')
    # TODO are there any global attributes that should be applied?  I don't think so, so we'll skip for now...

    # Note:  I do not think that we need to add the CDSVarGroup objects to the dataset (I couldn't see it being
    # set when I stepped through the ADI code), so we are leaving it out for now.

    # Now create the datastream group (I don't think we care what the name of the datastream is)
    datastream_group = cds3.Group.define(dataset_group, INPUT_DATASTREAM_NAME)

    # Now create the obs group
    obs_group = cds3.Group.define(datastream_group, 'obs1')

    # Now add dimensions to the obs
    dims = xr_input_var.sizes  # dict of dims and their lengths (i.e., {'time': 1440} )
    for dim_name in dims:
        # Note that we assume that time dimension will always be named 'time'
        is_unlimited = 1 if dim_name != 'time' else 0
        dim_size = dims[dim_name]
        obs_group.define_dim(dim_name, dim_size, is_unlimited)

    # Now add the coordinate variables to the obs group
    coords: DataArrayCoordinates = xr_input_var.coords
    for dim_name in coords.dims:
        dim_var = coords.get(dim_name)
        _add_variable_to_adi(dim_var, obs_group)

    # Now add the data variables to the obs group
    _add_variable_to_adi(xr_input_var, obs_group)
    _add_variable_to_adi(xr_input_qc_var, obs_group)

    return dataset_group


def _create_adi_transformed_dataset(xr_output_var: xr.DataArray, xr_output_qc_var: xr.DataArray) -> cds3.Group:
    """-----------------------------------------------------------------------------------------------------------------
    Create the following structure in ADI:

        Dataset Group: transformed_data
            Coordinate System Group: one_min  <-- dims go here (e.g., ['time'])
                cds3.core.Var <-- all coord vars go here
                Datastream Group: sbsaosmetS2.a1 <-- no dims here!
                   cds3.core.Var <-- all data vars go here (no coords!)

    Parameters
    ----------
    xr_output_var
    xr_output_qc_var

    Returns
    -------

    -----------------------------------------------------------------------------------------------------------------"""
    # First create the dataset group
    transformed_data = cds3.Group.define(None, 'transformed_data')

    # Now create the coordinate system group and add it to the transformed dataset
    cs_group = cds3.Group.define(transformed_data, COORDINATE_SYSTEM_NAME)

    # Now add the dimensions to the coordinate system group
    dims = xr_output_var.sizes  # dict of dims and their lengths (i.e., {'time': 1440} )
    for dim_name in dims:
        # Note that we assume that time dimension will always be named 'time'
        is_unlimited = 1 if dim_name != 'time' else 0
        dim_size = dims[dim_name]
        cs_group.define_dim(dim_name, dim_size, is_unlimited)

    # Now add the coordinate variables to the coordinate system group
    coords: DataArrayCoordinates = xr_output_var.coords
    for dim_name in coords.dims:
        dim_var = coords.get(dim_name)
        _add_variable_to_adi(dim_var, cs_group)

    # Now create the output datastream group and add it to the coordinate system  (note that trans datasets do not
    #   include obs groups!)
    ds_group = cds3.Group.define(cs_group, OUTPUT_DATASTREAM_NAME)

    # Now add the data variables to the datastream group
    _add_variable_to_adi(xr_output_var, ds_group)
    _add_variable_to_adi(xr_output_qc_var, ds_group)

    return transformed_data


def _add_atts_to_adi(xr_atts_dict: Dict, adi_obj: cds3.Object):
    # TODO: Convert over special attribute names to ADI names

    for att_name in xr_atts_dict:
        att_value = xr_atts_dict.get(att_name)
        cds_type = get_cds_type(att_value)
        status = dsproc.set_att(adi_obj, 1, att_name, cds_type, att_value)
        if status < 1:
            raise Exception(f'Could not create attribute {att_name}')


def _add_dims_to_adi():
    pass


def _add_variable_to_adi(xr_var: xr.DataArray, parent_group: cds3.Group, coordinate_system_name: str = None):
    """-----------------------------------------------------------------------------------------------------------------
    Add a variable specified by an xarray DataArray to the given ADI dataset.

    TODO: Do we need to add any VarTags?  I don't think so, since they are only used by dsproc, not libtrans.
        Vartags are:  source_ds_name, source_var_name, output_targets, and coordinate_system
    -----------------------------------------------------------------------------------------------------------------"""
    # First create the variable
    cds_type = get_cds_type(xr_var.data)
    dim_names = xr_dims = list(xr_var.dims)
    adi_var = dsproc.define_var(parent_group, xr_var.name, cds_type, dim_names)

    # Now assign attributes
    _add_atts_to_adi(xr_var.attrs, adi_var)

    # Now set the variable's data
    if xr_var.name == 'time':
        # If this is time, then we have to convert the values because xarray time is different
        _set_time_variable_data(xr_var, adi_var)
    else:
        # Just use the same data pointer to the numpy ndarray
        sample_count = xr_var.sizes[dim_names[0]]
        dsproc.attach_var_data(adi_var, xr_var.data, sample_count)

    # Add the coordinate system name to a VarTag object for the variable (not sure if we need this for transform)
    if coordinate_system_name:
        dsproc.set_var_coordsys_name(adi_var, coordinate_system_name)


def _set_time_variable_data(xr_var: xr.DataArray, adi_var: cds3.Var):
    """-----------------------------------------------------------------------------------------------------------------
    For time values, we actually have to create a copy.  We can't rely on the data pointer for time, because the times
    are converted into datetime64 objects for xarray.
    -----------------------------------------------------------------------------------------------------------------"""
    # astype will produce nanosecond precision, so we have to convert to seconds
    timevals = xr_var.data.astype('float') / 1000000000

    # We have to truncate to 6 decimal places so it matches ADI
    timevals = np.around(timevals, 6)

    # Set the timevals in seconds in ADI
    sample_count = xr_var.sizes[xr_var.dims[0]]
    dsproc.set_sample_timevals(adi_var, 0, sample_count, timevals)


def get_cds_type(value: Any) -> int:
    """-----------------------------------------------------------------------
    For a given Python data value, convert the data type into the corresponding
    ADI CDS data type.

    Args:
        value (Any): Can be a single value, a List of values, or a numpy.ndarray
            of values.

    Returns:
        int: The corresponding CDS data type
    -----------------------------------------------------------------------"""
    val = value

    # Convert value to a numpy array so we can use dsproc method which
    # only works if value is a numpy ndarray
    if type(value) == list:
        val = np.array(value)

    elif type(value) != np.ndarray:
        # We need to wrap value in a list so np constructor doesn't get confused
        # if value is numeric
        val = np.array([value])

    if val.dtype.type == np.str_:
        # This comparison is always failing from within the cython, because
        # in the cython, dtype.type = 85 instead of np.str_.
        # So I'm adding it here instead.  This checks for any string type.
        cds_type = cds3.CHAR

    else:
        cds_type = dsproc.dtype_to_cds_type(val.dtype)

    return cds_type
