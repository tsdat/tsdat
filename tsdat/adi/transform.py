import math
import re
from typing import Any, Dict, List

import xarray as xr
from xarray.core.coordinates import DataArrayCoordinates
import numpy as np

import cds3
import dsproc3 as dsproc
import trans


# We will always use the same coordinate system, input datastream, and output datastream name for every ADI dataset
# conversion, since tsdat only will allow one coordinate system and libtrans doesn't care what the names are.
COORDINATE_SYSTEM = 'coord_sys'
OUTPUT_DATASTREAM = 'output_ds'
INPUT_DATASTREAM = 'input_ds'


class ADITransformationTypes:
    # Allowed ADI transform algorithms - these are the values that can be used in transformation_type parameter
    TRANS_AUTO = 'TRANS_AUTO'
    TRANS_INTERPOLATE = 'TRANS_INTERPOLATE'
    TRANS_SUBSAMPLE = 'TRANS_SUBSAMPLE'
    TRANS_BIN_AVERAGE = 'TRANS_BIN_AVERAGE'
    TRANS_PASSTHROUGH = 'TRANS_PASSTHROUGH'


class ADIAlignments:
    LEFT = 'LEFT'
    CENTER = 'CENTER'
    RIGHT = 'RIGHT'

    label_to_int = {
        LEFT: 0,
        CENTER: .5,
        RIGHT: 1
    }

    @staticmethod
    def get_adi_value(parameter_value: str):
        return ADIAlignments.label_to_int.get(parameter_value)


class TransformParameterConverter:

    # Maps which type of object ADI needs to apply the transform parameters to
    transform_param_type = {
        "transformation_type": COORDINATE_SYSTEM,
        "width": COORDINATE_SYSTEM,
        "alignment": COORDINATE_SYSTEM,
        "range": INPUT_DATASTREAM,
        "qc_mask": INPUT_DATASTREAM,
        "missing_value": INPUT_DATASTREAM,
        "qc_bad": INPUT_DATASTREAM,
        "std_ind_max": COORDINATE_SYSTEM,
        "std_bad_max": COORDINATE_SYSTEM,
        "goodfrac_ind_min": COORDINATE_SYSTEM,
        "goodfrac_bad_min": COORDINATE_SYSTEM
    }
    
    def convert_to_adi_format(self, pcm_transform_parameters: Dict) -> Dict[str, str]:
        transforms = {}

        for parameter_name, transform_parameter in pcm_transform_parameters.items():
            parameter_type = self.transform_param_type.get(parameter_name)
            transform_parameter_name = self._get_adi_transform_parameter_name(parameter_name, parameter_type)

            # Check if the transform parameter object is technically empty - if it is, then just skip it
            if self._is_transform_parameter_empty(transform_parameter):
                continue

            # First find any coordinate system defaults and add a row to the coordinate system file
            coordinate_system_defaults = transform_parameter.get('coordinate_system_defaults', [])
            for coord_system_default in coordinate_system_defaults:
                file_name = COORDINATE_SYSTEM
                dim_name = coord_system_default.get('dim')
                value = coord_system_default.get('default_value')
                self._write_transform_parameter_row(transforms, file_name, None, dim_name, transform_parameter_name,
                                                    value)

            # Then find any input datastream defaults and add a row to the input datastream file
            input_datastream_defaults = transform_parameter.get('input_datastream_defaults', [])
            for datastream_default in input_datastream_defaults:
                file_name = INPUT_DATASTREAM
                dim_name = datastream_default.get('dim')
                value = datastream_default.get('default_value')
                self._write_transform_parameter_row(transforms, file_name, None, dim_name, transform_parameter_name,
                                                    value)

            # Then loop through all the variables and add a row to either the input_datastream file or coordinate_system
            # file, depending upon the type
            variables = transform_parameter.get('variables', [])
            for variable in variables:
                variable_name = variable['name']
                dim_name = variable['dim']
                value = variable['value']

                if parameter_type == COORDINATE_SYSTEM:
                    # Use our special coordinate system as the file name
                    file_name = COORDINATE_SYSTEM
                else:
                    # Use our special input datastream as the file name
                    file_name = INPUT_DATASTREAM

                self._write_transform_parameter_row(transforms, file_name, variable_name, dim_name,
                                                    transform_parameter_name, value)

        return transforms

    def _write_transform_parameter_row(self, transforms: Dict[str, str], file_name: str, base_var_name: str,
                                       dim_name: str, parameter_name:str, value: str):

        # ADI transforms requires that the qc_ variable name is used instead of the actual variable name, so we need
        # to append it here
        variable_name = base_var_name
        if parameter_name == 'qc_bad' or parameter_name == 'qc_mask':
            if base_var_name and base_var_name[0:3] != 'qc_':
                variable_name = f'qc_{base_var_name}'

        elif parameter_name == 'alignment':
            value = ADIAlignments.get_adi_value(value)

        if parameter_name == 'range' and value == 'LENGTH_OF_PROCESSING_INTERVAL':
            # If this parameter is range and value is LENGTH_OF_PROCESSING_INTERVAL, then we can't save the parameter
            # because ADI doesn't recognize LENGTH_OF_PROCESSING_INTERVAL as a valid option.
            print('Omitting range=LENGTH_OF_PROCESSING_INTERVAL since it is not recognized by ADI and is the default.')

        else:
            # If this is qc_mask parameter, then we have to convert the value from a binary string to integer
            if parameter_name == 'qc_mask':
                value = self._convert_bit_positions_to_integer(value)

            elif parameter_name == 'qc_bad':
                value = ", ".join(value)

            if file_name not in transforms:
                transforms[file_name] = ''

            row_text = f'{parameter_name} = {value};'

            # If dim is null, then it was deliberately set that way, so we should not include it in the file
            if dim_name:
                row_text = f'{dim_name}:{row_text}'
            if variable_name:
                row_text = f'{variable_name}:{row_text}'

            # Append the current row to the existing text
            existing_text = transforms[file_name]
            transforms[file_name] = f'{existing_text}{row_text}\n'

    def _convert_bit_positions_to_integer(self, bit_position_array):
        """
        Convert an array of bit positions starting at bit 1 for the zeroeth bit (ie., [1,3]) into an
        integer with the proper bits flipped.
        """
        int_value = 0
        for bit_position in bit_position_array:  # ie., [1,3]
            power = int(bit_position) - 1
            int_value += int(math.pow(2, power))

        return int_value

    def _is_transform_parameter_empty(self, parameter):

        has_coordinate_system_defaults = True if parameter.get('coordinate_system_defaults') else False
        has_input_datastream_defaults = True if parameter.get('input_datastream_defaults') else False
        has_variable_overrides = True if parameter.get('variables') else False

        empty = not has_coordinate_system_defaults and not has_input_datastream_defaults and not has_variable_overrides
        return empty

    def _get_adi_transform_parameter_name(self, parameter_name: str, file_type: str):
        """
        Convert transform parameter name from PCM format to names used in adi transform files.
        """
        name = parameter_name.strip().lower()

        if name == 'transformation_type':
            name = 'transform'  # We use a different name in our UI than in the file

        elif file_type == 'input_datastream' and name == 'input_datastream_alignment':
            name = 'alignment'

        elif file_type == 'input_datastream' and name == 'input_datastream_width':
            name = 'width'

        return name


class AdiTransformer:

    def transform_new(self,
                      input_dataset: xr.Dataset,
                      output_dataset: xr.Dataset,
                      transform_parameters: Dict
                      ):
        """-------------------------------------------------------------------------------------------------------------
        This function will use ADI libraries to transform one data variable to the shape defined for the output.
        This function will also fill out the output qc_ variable with the appropriate qc status from the transform
        algorithm.

        The output variable and output qc_ variables' data will be written in place.  Any variable attribute values
        that were added by adi will be copied back to the output variables.

        Caller does not need to call this transform method for any 1-d variables where TRANS_PASSTHROUGH would apply.
        However, if there are two dimensions (say time and height), and the user only wants to transform one dimension
        (for example, time data is mapped to the input, but height data needs to be averaged), then you would need to
        call this transform and use TRANS_PASSTHROUGH for all the mapped dimension and a different transformation
        algorithm for any non-mapped dimensions.

        If all dimensions are mapped and caller does not call this method, then all input values and input qc values
        must be copied over to the output by the caller.  Also in this case the caller should add a 'source' attribute
        on the variable to explain what datastream the value came from.

        Parameters
        ----------
        input_dataset : xarray.Dataset
            An xarray dataset containing:
            1) A data variable to be transformed
            2) Zero or one qc_variable that contains qc flags for the data variable.  The qc_ variable must have the
                exact same base name as the data variable.  For example, if the data variable is named 'temperature',
                then the qc variable must be named qc_temperature.
            3) One or more coordinate variables matching the coordinates on the data variable
            4) Zero or more bounds variables, one for each coordinate variable.  Bounds variables specify the front
                edge and back edge of the bins used to compute the input data values.  If no bounds variables are
                provided, ADI will assume each data point is a single, instantaneous value.  If bounds variables
                are not present in the input data files, if the user knows what the bin widths and alignments were for
                the input datastreams, they can specify these values via the width and alignment transformation
                parameters (note that these parameters are for the input datastreams, not coordinate system defaults).

            * Note that if range transform parameters were set on any datastreams, the xarray data must have at least
             'range' amount of extra data retrieved before and after the day being processed (if it exists).

            * Note that the input variables should have been renamed to use the name from the output dataset.  So
                we should rename input variables to match their output names BEFORE we pass them to this method.

            * Note that variable dimensions should have been renamed to match their names in the output variable.

        output_dataset : xarray.Dataset
            An xarray dataset where the transformed data will go.  The output dataset must contain:
            1) One or more coordinate variables with the shape of the defined output
            2) One empty data variable with the same shape as its coordinate variables.  The transformed values will be
                filled in by ADI.
            3) One empty qc variable with the same shape as its coordinate variables.  The qc flags and bit metadata
                attributes will be filled in by this function
            4) One or more bounds variables, one for each coordinate variable.  The bounds variables will contain the
                front edge and back edge of each bin for each output coord data point.  The bounds variable values can
                computed from the coordinate data points and the width and alignment transformation parameters.

                If the user does not specify bin width or alignment, then we use CENTER alignment by default and we
                compute the bid width as the median of all the deltas between coordinate points.

        transform_parameters : Dict

            A compressed set of transformation parameters that apply just to the specific data variable being
            transformed.  The following is the minimal set used for our initial release (more ADI parameters can be
            added later, as they will be supported by the back end).

            transform_parameters = {

                # Transformation_type defines the algorithm to use. This parameter should be defined by the converter.
                # Valid values are:
                #     TRANS_AUTO (This will average if there are more input points than output points, otherwise, interpolate)
                #     TRANS_INTERPOLATE
                #     TRANS_SUBSAMPLE   (i.e., nearest neighbor)
                #     TRANS_BIN_AVERAGE
                #     TRANS_PASSTHROUGH (all values passed directly through from the input, no transform takes place)

                "transformation_type": {
                    "time": "TRANS_AUTO"
                },

                # Range specifies how far the transformer should look for the next good value when performing
                # subsample or interpolate transforms.
                # Range is always in same units as coord (e.g., seconds in this case).
                #  * Note that if range transform parameters are set for any datastreams, the xarray data must have at
                #  least 'range' amount of extra data retrieved before and after the day being processed (if it exists)!
                "range": {
                    "time": 1800
                },

                # Width applies only when using bin averaging, and it specifies the width of the bin that was used to
                # determine a specific point.  Width is always in same units as coord.
                # Only use width if user wants to make the bin width different than the delta between points (e.g., for
                # smoothing data)
                "width": {
                    "time": 600
                }

                # Alignment applies only when using bin averaging, and it specifies where in the bin the data point is
                # located.  Valid values are:
                #    LEFT
                #    CENTER
                #    RIGHT
                # Default is CENTER
                "alignment": {
                    "time": LEFT
                }
            }

            TODO: Longer term, we should support front edge/back edge, width, and/or alignment parameters for input
                datastreams in order to provide more specific bounds on the data points.  For now we won't set these and
                the transformer will use the default values, which is to assume each data point is instantaneous and
                there is no bin.

        Returns
        -------
        Void - transforms are done in-place on output_dataset
        -------------------------------------------------------------------------------------------------------------"""
        # TODO: Update the main algorithm to use datasets instead of input/output variables
        # TODO: Fix bug where qc attributes were added for non-qc variables
        # TODO: Update the TransformParameterConverter to use this new streamlined format
        # TODO: If the input or output dataset has bounds variables, then we must convert these to front edge/back edge
        #   transformation parameters.
        pass

    def transform(self, 
                  input_var: xr.DataArray,
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
            Transformation params in streamlined PCM json format.  We don't need all the same details because
            there will only be one input datastream and one coordinate system:

            {
              "transformation_type": {
                "name": "transformation_type",
                "coordinate_system_defaults": [
                  {
                    "dim": "range",
                    "default_value": "TRANS_AUTO"
                  },
                  {
                    "dim": "time",
                    "default_value": "AUTO"
                  }
                ],
                "input_datastream_defaults": [],
                "variables": [
                  {
                    "name": "ceil_backscatter",
                    "value": "TRANS_PASSTHROUGH",
                    "dim": "time"
                  },
                  {
                    "name": "met_cmh_vapor_pressure",
                    "value": "TRANS_SUBSAMPLE",
                    "dim": "time"
                  }
                ]
              },
              "range": {
                "name": "range",
                "coordinate_system_defaults": [],
                "input_datastream_defaults": [
                  {
                    "dim": "time",
                    "default_value": "600"
                  }
                ],
                "variables": [
                  {
                    "name": "met_cmh_vapor_pressure",
                    "value": "300",
                    "dim": "time"
                  }
                ]
              }
            }

            TODO: Longer term, we should support front edge/back edge, width, and/or alignment parameters for input
                datastreams in order to provide more specific bounds on the data points.  For now we won't set these and the
                transformer will use the default values, which is to assume center alignment and the front and back edge
                are the previous andsubsequent points, respectively.
    
        -----------------------------------------------------------------------------------------------------------------"""
    
        # First convert the input and output variables into ADI format
        retrieved_dataset: cds3.Group = self._create_adi_retrieved_dataset(input_var, input_qc_var)
        transformed_dataset: cds3.Group = self._create_adi_transformed_dataset(output_var, output_qc_var)
    
        # Now convert the tranform parameters into ADI format
        adi_transform_parameters = TransformParameterConverter().convert_to_adi_format(pcm_transform_parameters)
    
        # Now apply the coordinate system transform parameters to the coordinate system group
        if COORDINATE_SYSTEM in adi_transform_parameters:
            params = adi_transform_parameters.get(COORDINATE_SYSTEM)
            cs_group = transformed_dataset.get_groups()[0]
            cds3.parse_transform_params(cs_group, params)
    
        # now apply the input datastream transform parameters to the obs group
        if INPUT_DATASTREAM in adi_transform_parameters:
            params = adi_transform_parameters.get(INPUT_DATASTREAM)
            obs_group = retrieved_dataset.get_groups()[0].get_groups()[0]
            cds3.parse_transform_params(obs_group, params)
    
        # Now run the transform
        adi_input_var = retrieved_dataset.get_groups()[0].get_groups()[0].get_var(input_var.name)
        adi_input_qc_var = retrieved_dataset.get_groups()[0].get_groups()[0].get_var(input_qc_var.name)
        adi_output_var = transformed_dataset.get_groups()[0].get_groups()[0].get_var(output_var.name)
        adi_output_qc_var = transformed_dataset.get_groups()[0].get_groups()[0].get_var(output_qc_var.name)
        trans.transform_driver(adi_input_var, adi_input_qc_var, adi_output_var, adi_output_qc_var)
    
        # Now copy any changed variable attributes back to the xr out variables.
        self._update_xr_attrs(output_var, adi_output_var)
        self._update_xr_attrs(output_qc_var, adi_output_qc_var)
    
        # Now free up memory from created adi data structures
        self._free_memory(retrieved_dataset)
        self._free_memory(transformed_dataset)
    
    def _free_memory(self, adi_dataset: cds3.Group):
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
                    var.detach_data()
    
        detatch_vars(adi_dataset)
    
        #  After all the variable data has been detached, then we can delete the group.
        cds3.Group.delete(adi_dataset)
    
    def _create_adi_retrieved_dataset(self, xr_input_var: xr.DataArray, xr_input_qc_var: xr.DataArray) -> cds3.Group:
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
        datastream_group = cds3.Group.define(dataset_group, INPUT_DATASTREAM)
    
        # Now create the obs group
        obs_group = cds3.Group.define(datastream_group, 'obs1')
    
        # Now add dimensions to the obs
        dims = xr_input_var.sizes  # dict of dims and their lengths (i.e., {'time': 1440} )
        for dim_name in dims:
            # Note that we assume that time dimension will always be named 'time'
            is_unlimited = 1 if dim_name == 'time' else 0
            dim_size = dims[dim_name]
            obs_group.define_dim(dim_name, dim_size, is_unlimited)
    
        # Now add the coordinate variables to the obs group
        coords: DataArrayCoordinates = xr_input_var.coords
        for dim_name in coords.dims:
            dim_var = coords.get(dim_name)
            self._add_variable_to_adi(dim_var, obs_group)
    
        # Now add the data variables to the obs group
        self._add_variable_to_adi(xr_input_var, obs_group)
        self._add_variable_to_adi(xr_input_qc_var, obs_group)
    
        return dataset_group
    
    def _create_adi_transformed_dataset(self, 
                                        xr_output_var: xr.DataArray, xr_output_qc_var: xr.DataArray) -> cds3.Group:
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
        cs_group = cds3.Group.define(transformed_data, COORDINATE_SYSTEM)
    
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
            self._add_variable_to_adi(dim_var, cs_group)
    
        # Now create the output datastream group and add it to the coordinate system  (note that trans datasets do not
        #   include obs groups!)
        ds_group = cds3.Group.define(cs_group, OUTPUT_DATASTREAM)
    
        # Now add the data variables to the datastream group
        self._add_variable_to_adi(xr_output_var, ds_group, coordinate_system_name=COORDINATE_SYSTEM)
        self._add_variable_to_adi(xr_output_qc_var, ds_group, coordinate_system_name=COORDINATE_SYSTEM)
    
        return transformed_data

    def _update_xr_attrs(self, xr_var: xr.DataArray, adi_var: cds3.Var):
        adi_atts: List[cds3.Att] = adi_var.get_atts()
        xr_attrs = {}
        qc_attrs = {}

        # We have to back-convert any qc variables
        for att in adi_atts:
            att_name = att.get_name()
            att_value = dsproc.get_att_value(adi_var, att.get_name(), att.get_type())
            if att_name.startswith('bit_'):
                qc_attrs[att_name] = att_value
            elif att_name != 'flag_method':
                xr_attrs[att_name] = att_value

        xr_attrs.update(self._back_convert_qc_atts(qc_attrs))

        for name, value in xr_attrs.items():
            xr_var.attrs[name] = value
    
    def _add_atts_to_adi(self, xr_atts_dict: Dict, adi_obj: cds3.Object):

        atts = xr_atts_dict

        # If this is a qc variable, then we need to convert over the qc attributes
        if adi_obj.get_name().startswith('qc_'):
            # Use a new dictionary of atts so we don't mess with the attributes for the original data array
            atts = {}
            qc_atts = {}

            # Collect the qc atts together, since we need to convert them together
            for att_name, att_value in xr_atts_dict.items():
                if att_name == 'flag_masks' or att_name == 'flag_meanings' or att_name == 'flag_assessments':
                    qc_atts[att_name] = att_value
                else:
                    atts[att_name] = att_value

            # Convert the qc atts to ADI format (Tsdat uses the condensed ACT format)
            atts.update(self._convert_qc_atts(qc_atts))

            # We also have to add the flag_method att
            atts['flag_method'] = 'bit'

        # Now add the atts to ADI
        for att_name, att_value in atts.items():
            cds_type = self._get_cds_type(att_value)
            status = dsproc.set_att(adi_obj, 1, att_name, cds_type, att_value)
            if status < 1:
                raise Exception(f'Could not create attribute {att_name}')

    def _back_convert_qc_atts(self, adi_qc_atts: Dict):
        """-------------------------------------------------------------------------------------------------------------
        We are converting from exploded ADI format:
            bit_1_description = "Value is equal to _FillValue or NaN"
            bit_1_assessment = "Bad"
            bit_2_description = "Value is less than the valid_min."
            bit_2_assessment = "Bad
            etc...

        to condensed ACT format:
            flag_masks = 1U, 2U, 4U ;
            flag_meanings = "Value is equal to _FillValue or NaN", "Value is less than the valid_min.", "Value is greater than the valid_max." ;
            flag_assessments = "Bad", "Bad", "Bad" ;

        Parameters
        ----------
        adi_qc_atts : Dict
            Dictionary of exploded attributes used by ADI

        Returns
        -------
        Dict
            Dictionary of 3 condensed qc attributes used by ACT (and Tsdat)
        -------------------------------------------------------------------------------------------------------------"""
        bit_metadata = {}
        bit_pattern = re.compile(r'^bit_(\d+)_(.+)$')

        # First we build up a dictionary sorted by bit number
        for att_name, att_value in adi_qc_atts.items():
            match = bit_pattern.match(att_name)
            if match:
                bit_number = match.groups()[0]
                att_type = match.groups()[1]

                if bit_number not in bit_metadata:
                    bit_metadata[bit_number] = {}

                bit_metadata[bit_number][att_type] = att_value

        # Make sure the keys are sorted in numerical order of the bit numbers
        sorted_bit_numbers = sorted(bit_metadata.keys())

        # Now build our arrays
        flag_masks = []
        flag_meanings = []
        flag_assessments = []
        for bit_number in sorted_bit_numbers:
            metadata = bit_metadata.get(bit_number)
            power = int(bit_number) - 1
            mask = int(math.pow(2, power))
            flag_masks.append(mask)
            flag_meanings.append(metadata.get('description'))
            flag_assessments.append(metadata.get('assessment'))

        xr_qc_atts = {
            'flag_masks': flag_masks,
            'flag_meanings': flag_meanings,
            'flag_assessments': flag_assessments
        }
        return xr_qc_atts

    def _convert_qc_atts(self, xr_qc_atts: Dict):
        """-------------------------------------------------------------------------------------------------------------
        We are converting from condensed ACT format:
            flag_masks = 1U, 2U, 4U ;
            flag_meanings = "Value is equal to _FillValue or NaN", "Value is less than the valid_min.", "Value is greater than the valid_max." ;
            flag_assessments = "Bad", "Bad", "Bad" ;

        to exploded ADI format:
            bit_1_description = "Value is equal to _FillValue or NaN"
            bit_1_assessment = "Bad"
            bit_2_description = "Value is less than the valid_min."
            bit_2_assessment = "Bad
            etc...

        Parameters
        ----------
        xr_qc_atts : Dict
            Dictionary of 3 condensed qc attributes used by ACT

        Returns
        -------
        Dict
            Dictionary of exploded attributes used by ADI
        -------------------------------------------------------------------------------------------------------------"""

        flag_masks = xr_qc_atts.get('flag_masks', [])
        flag_meanings = xr_qc_atts.get('flag_meanings', [])
        flag_assessments = xr_qc_atts.get('flag_assessments', [])

        # Note that I think ADI may crash if the bit numbers are not contiguous
        adi_qc_atts = {}
        for i in range(len(flag_masks)):
            # Convert the integer to a bit position starting from 1
            bit_number = str(int(math.log2(flag_masks[i])) + 1)
            adi_qc_atts[f'bit_{bit_number}_description'] = flag_meanings[i]
            adi_qc_atts[f'bit_{bit_number}_assessment'] = flag_assessments[i]

        return adi_qc_atts

    def _add_variable_to_adi(self, xr_var: xr.DataArray, parent_group: cds3.Group, coordinate_system_name: str = None):
        """-----------------------------------------------------------------------------------------------------------------
        Add a variable specified by an xarray DataArray to the given ADI dataset.
    
        TODO: Do we need to add any VarTags?  I don't think so, since they are only used by dsproc, not libtrans.
            Vartags are:  source_ds_name, source_var_name, output_targets, and coordinate_system
        -----------------------------------------------------------------------------------------------------------------"""
        # First create the variable
        cds_type = self._get_cds_type(xr_var.data)
        dim_names = xr_dims = list(xr_var.dims)
        adi_var = dsproc.define_var(parent_group, xr_var.name, cds_type, dim_names)
    
        # Now assign attributes
        self._add_atts_to_adi(xr_var.attrs, adi_var)
    
        # Now set the variable's data
        if xr_var.name == 'time':
            # If this is time, then we have to convert the values because xarray time is different
            self._set_time_variable_data(xr_var, adi_var)
        else:
            # Just use the same data pointer to the numpy ndarray
            sample_count = xr_var.sizes[dim_names[0]]
            adi_var.attach_data(xr_var.data.__array_interface__['data'][0], sample_count)
    
        # Add the coordinate system name to a VarTag object for the variable (not sure if we need this for transform)
        if coordinate_system_name:
            dsproc.set_var_coordsys_name(adi_var, coordinate_system_name)
     
    def _set_time_variable_data(self, xr_var: xr.DataArray, adi_var: cds3.Var):
        """-----------------------------------------------------------------------------------------------------------------
        For time values, we actually have to create a copy.  We can't rely on the data pointer for time, because the times
        are converted into datetime64 objects for xarray.
        -----------------------------------------------------------------------------------------------------------------"""
        # astype will produce nanosecond precision, so we have to convert to seconds
        timevals = xr_var.data.astype('float') / 1000000000
    
        # We have to truncate to 6 decimal places so it matches ADI
        timevals = np.around(timevals, 6)
    
        # Set the timevals in seconds in ADI
        dsproc.set_sample_timevals(adi_var, 0, timevals)
    
    def _get_cds_type(self, value: Any) -> int:
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
