from enum import Enum
from typing import Any, Dict, List

import xarray as xr
import numpy as np

import cds3
import dsproc3 as dsproc

# We will always use the same coordinate system name for every ADI dataset conversion, since Tsdat will only allow the
#   user to define one coordinate system.
COORDINATE_SYSTEM_NAME = 'coord_sys'

# I don't think the ADI transformer cares what the output datastream name is, so we are going to hardcode it for now
OUTPUT_DATASTREAM_NAME = 'output_ds'


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


def transform(transform_parameters: Dict, input_datasets: List[xr.Dataset], output_dataset: xr.Dataset):
    """-----------------------------------------------------------------------------------------------------------------
    This function will use ADI libraries to transform data variables found in the output_dataset based upon values from
    the input datasets and add appropriate QC flags to their corresponding QC variables.  The transformed and QC values
    will be written in place.  Any variable or global attribute values that were added or changed by the adi transformer
    will be copied back to the  output dataset.

    Parameters
    ----------
    transform_parameters : Dict
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

    input_datasets : List[xr.Dataset]
        Set of xarray datasets for the input datastreams

        * Note that if range transform parameters were set on any datastreams, the xarray datasets much have at least
         'range' amount of extra data retrieved before and after the day being processed (if it exists).

        * Note that we should run units converters BEFORE passing the input datasets to the transform.

        * Note that variables from the original input datasets need to have the name used in the output dataset.  So
            we should rename input variables to match their output names BEFORE we pass them to this method.

            i.e.,
            ds1
              varA <-- retrieved name, not the name from the original file
              varB <-- retrieved name, not the name from the original file

            ds2
              varC <-- retrieved name, not the name from the original file
              varD <-- retrieved name, not the name from the original file

    output_dataset : xr.Dataset
        Xarray dataset for the output.  It should have the coordinate variables set (We know this because the shape of
        the output will be defined in the tsdat config file.  Shape could be mapping or regular interval.)

        It should have placeholder variables created for all the data and qc variables (every data variable must have
        a qc variable created).  All the values can be missing or Nan - doesn't matter because transform code will
        overwrite them.

        * We also need to create the bounds vars for the coordinates

    Returns
    -------

    """
    """
    Steps:
        1) Convert all of the xarray input datasets to CDSGroup objects.  We will need Groups for input datastream,
            obs (should be just one because we just have one file), and coordinate system.  Input datastreams and
            coord sys need to go into the retrieved dataset group in ADI.
    
            Retrieved data structure
            
            Retrieved Dataset (CDSGroup)    
                    CoordinateSystem
                    Input datastream[]  (one or more input datastream)         
                        obs[]                                
                            obs 0 (all the data that came from file 1)
                                CDSVar 1 (pointer to the coord sys)
                                CDSVar 2 (pointer to the coord sys)
                            obs 1 (all the data that came from file 2)
    
            * Note tsdat does not make users define a coordinate system name, so we will use 'coord_sys' or similar since
                we only allow 1 coordinate system per process.
    
            * Note:  this must be done in cython
    
        2) Convert the output xarray object into a CDS Group set of objects (same as above)
            Output xarray dataset needs to go into the transformed dataset group in ADI
    
        3) Convert the transform parameters into a map of strings.  Key is the data stream or coordinate system name, the
           value is transform parameters string in ADI format.
    
        4) For each input datastream, see if there are transform params in the map.  If so, then we run
            cds_parse_transform_params on the input datastream CDSGroup and pass in the string.
    
        5) See if there is a coord_sys entry in the map, and if so, then run cds_parse_transform_params on the coordinate
            system CDSGroup and pass in the string.
    
        6) Iterate through the data variables and call  cds_transform_driver on them.
            int cds_transform_driver(CDSVar *invar, CDSVar *qc_invar, CDSVar *outvar, CDSVar *qc_outvar)
    
        7) Convert the output CDS structure back to Xarray format (same as what we do with adi_py, so we share the data
            pointer and we only have to copy over attributes that changed).
        
        8) I think we need to delete all the groups we created so we can free up the ADI memory, but DO NOT delete
            any data structures for the variables since we are using shared pointers!
    """
    # First convert the input and output datasets into ADI format
    retrieved_dataset = create_adi_retrieved_dataset(input_datasets)
    output_dataset = create_adi_transformed_dataset(output_dataset)

    # Now convert the tranform parameters into ADI format

    # Now apply the transform parameters to the appropriate ADI groups


def create_adi_retrieved_dataset(xr_datasets: List[xr.Dataset]):
    # First create the dataset group
    retrieved_data = cds3.Group.define(None, 'retrieved_data')

    # Note:  I do not think that we need to add the CDSVarGroup objects to the dataset (used for quickly referencing
    #   all variables as an array), since I don't believe this is used by the transform logic, so we are leaving it
    #   out for now.

    # Now add all the datastreams to the dataset group (one per xr_dataset)
    for xr_dataset in xr_datasets:
        # Now add one observation to the datastream group
        # Now add one dimension to the obs group
        # Add the global atts to the obs group

        # Now add each data variable to the obs group
        for xr_var in xr_dataset.data_vars:
            # Now add the dimension to the variable
            # Add the variable atts to the variable
            pass

        pass


def create_adi_transformed_dataset(xr_dataset: xr.Dataset):
    # First create the dataset group
    transformed_data = cds3.Group.define(None, 'transformed_data')

    # Now create the coordinate system group and add it to the transformed dataset
    trans_cs_group = cds3.Group.define(transformed_data, COORDINATE_SYSTEM_NAME)

    # Now create the output datastream group and add it to the coordinate system
    trans_ds_group = cds3.Group.define(trans_cs_group, OUTPUT_DATASTREAM_NAME)

    # TODO: does trans datastream group have obs?  Need to double check

    # Now add all variables to the datastream group
    for xr_var in xr_dataset.variables:
        pass

    """
                *trans_var = _dsproc_copy_ret_var_to_trans_group(
                ret_var, 0, NULL, NULL,
                trans_obs_group, CDS_NAT, NULL, 0, 0);

            /* Create the QC variable in the transformation group. */
        trans_qc_var = _dsproc_create_trans_qc_var(
            trans_obs_group, *trans_var, is_caracena);

    """


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

        cds_type = dsproc.dtype_to_cds_type(val.dtype)

    return cds_type


def _add_variable_to_adi(xr_var: xr.DataArray, adi_dataset: cds3.Group, coordinate_system_name: str = None):
    """-----------------------------------------------------------------------------------------------------------------
    Add a variable specified by an xarray DataArray to the given ADI dataset.
    -----------------------------------------------------------------------------------------------------------------"""
    # First create the variable
    cds_type = get_cds_type(xr_var.data)
    dim_names = xr_dims = list(xr_var.dims)
    adi_var = dsproc.define_var(adi_dataset, xr_var.name, cds_type, dim_names)

    # Now assign attributes
    _sync_attrs(xr_var.attrs, adi_var)

    # Now set the variable's data
    if xr_var.name == 'time':
        # If this is time, then we have to convert the values because xarray time is different
        _set_time_variable_data(xr_var, adi_var)
    else:
        # Just use the same data pointer to the numpy ndarray
        xr_pointer = xr_var.data.__array_interface__['data'][0]
        # adi_var->data.vp = xr_pointer;

    # Add the coordinate system name
    if coordinate_system_name:
        dsproc.set_var_coordsys_name(adi_var, coordinate_system_name)

    # We don't care about the output targets, so we aren't adding those var tags


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

    # TODO: do we need to set the basetime variable too?


def _sync_attrs(xr_atts_dict: Dict, adi_obj: cds3.Object):
    """-----------------------------------------------------------------------
    Sync Xarray attributes back to an ADI object (dataset or variable) by
    checking if the following changes were made:

        - Attribute values changed
        - Attributes were added
        - Attributes were deleted
        - An attribute type changed

    Args:
        xr_atts (Dict):
            Dictionary of Xarray attributes, where the keys are
            attribute names, and values are attribute values

        adi_obj (cds3.Object):
            ADI dataset or variable
    -----------------------------------------------------------------------"""
    # Get lists of attribute names for comparison between two lists
    adi_atts = {att.get_name() for att in adi_obj.get_atts()}
    xr_atts = []

    for att_name in xr_atts_dict:
        if att_name.startswith('__'):
            # special attributes start with '__' and are handled separately
            continue
        xr_atts.append(att_name)

    # First remove deleted atts
    deleted_atts = [att_name for att_name in adi_atts if att_name not in xr_atts]
    for att_name in deleted_atts:
        adi_att = dsproc.get_att(adi_obj, att_name)
        status = cds3.Att.delete(adi_att)
        if status < 1:
            raise Exception(f'Could not delete attribute {att_name}')

    # Then add new atts
    added_atts = [att_name for att_name in xr_atts if att_name not in adi_atts]
    for att_name in added_atts:
        att_value = xr_atts_dict.get(att_name)
        cds_type = get_cds_type(att_value)
        status = dsproc.set_att(adi_obj, 1, att_name, cds_type, att_value)
        if status < 1:
            raise Exception(f'Could not create attribute {att_name}')

    # Next change the value for other atts if the value changed
    other_atts = [att_name for att_name in xr_atts if att_name not in added_atts]
    for att_name in other_atts:
        att_value = xr_atts_dict.get(att_name)

        # For now, if the att is already defined in adi, we assume that the user will not
        # change the type, just the value.
        cds_type = dsproc.get_att(adi_obj, att_name).get_type()
        existing_value = dsproc.get_att_value(adi_obj, att_name, cds_type)

        if not np.array_equal(att_value, existing_value):
            status = dsproc.set_att(adi_obj, 1, att_name, cds_type, att_value)
            if status < 1:
                raise Exception(f'Could not update attribute {att_name}')
