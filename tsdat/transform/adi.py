import math
import re
from typing import Any, Dict, List
import warnings

import xarray as xr
from xarray.core.coordinates import DataArrayCoordinates
import numpy as np

try:
    import cds3
    import dsproc3 as dsproc
    import trans

    CDSObject = cds3.Object
    CDSGroup = cds3.Group
    CDSVar = cds3.Var
except ImportError:
    warnings.warn(
        "Warning: ADI libraries are not installed. Some time series transformation"
        " functions may not work."
    )
    cds3 = None
    dsproc = None
    trans = None

    CDSGroup = Any
    CDSVar = Any
    CDSObject = Any


# We will always use the same coordinate system, input datastream, and output datastream name for every ADI dataset
# conversion, since tsdat only will allow one coordinate system and libtrans doesn't care what the names are.
COORDINATE_SYSTEM = "coord_sys"
OUTPUT_DATASTREAM = "output_ds"
INPUT_DATASTREAM = "input_ds"

adi_qc_atts = {
    "bit_1_description": "QC_BAD:  Transformation could not finish, value set to missing_value.",
    "bit_1_assessment": "Bad",
    "bit_2_description": "QC_INDETERMINATE:  Some, or all, of the input values used to create this output value had a QC assessment of Indeterminate.",
    "bit_2_assessment": "Indeterminate",
    "bit_3_description": "QC_INTERPOLATE:  Indicates a non-standard interpolation using points other than the two that bracket the target index was applied.",
    "bit_3_assessment": "Indeterminate",
    "bit_4_description": "QC_EXTRAPOLATE:  Indicates extrapolation is performed out from two points on the same side of the target index.",
    "bit_4_assessment": "Indeterminate",
    "bit_5_description": "QC_NOT_USING_CLOSEST:  Nearest good point is not the nearest actual point.",
    "bit_5_assessment": "Indeterminate",
    "bit_6_description": "QC_SOME_BAD_INPUTS:  Some, but not all, of the inputs in the averaging window were flagged as bad and excluded from the transform.",
    "bit_6_assessment": "Indeterminate",
    "bit_7_description": "QC_ZERO_WEIGHT:  The weights for all the input points to be averaged for this output bin were set to zero.",
    "bit_7_assessment": "Indeterminate",
    "bit_8_description": "QC_OUTSIDE_RANGE:  No input samples exist in the transformation region, value set to missing_value.",
    "bit_8_assessment": "Bad",
    "bit_9_description": "QC_ALL_BAD_INPUTS:  All the input values in the transformation region are bad, value set to missing_value.",
    "bit_9_assessment": "Bad",
    "bit_10_description": "QC_BAD_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_bad_max.",
    "bit_10_assessment": "Bad",
    "bit_11_description": "QC_INDETERMINATE_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_ind_max.",
    "bit_11_assessment": "Indeterminate",
    "bit_12_description": "QC_BAD_GOODFRAC:  Fraction of good and indeterminate points over averaging interval are less than limit set by transform parameter goodfrac_bad_min.",
    "bit_12_assessment": "Bad",
    "bit_13_description": "QC_INDETERMINATE_GOODFRAC:  Fraction of good and indeterminate points over averaging interval is less than limit set by transform parameter goodfrac_ind_min.",
    "bit_13_assessment": "Indeterminate",
}


class ADITransformationTypes:
    # Allowed ADI transform algorithms - these are the values that can be used in transformation_type parameter
    TRANS_AUTO = "TRANS_AUTO"
    TRANS_INTERPOLATE = "TRANS_INTERPOLATE"
    TRANS_SUBSAMPLE = "TRANS_SUBSAMPLE"
    TRANS_BIN_AVERAGE = "TRANS_BIN_AVERAGE"
    TRANS_PASSTHROUGH = "TRANS_PASSTHROUGH"


class ADIAlignments:
    LEFT = "LEFT"
    CENTER = "CENTER"
    RIGHT = "RIGHT"

    label_to_int = {LEFT: 0, CENTER: 0.5, RIGHT: 1}

    @staticmethod
    def get_adi_value(parameter_value: str):
        return ADIAlignments.label_to_int.get(parameter_value)


class TransformParameterConverter:
    # Maps which type of object ADI needs to apply the transform parameters to
    transform_param_type = {
        "transformation_type": COORDINATE_SYSTEM,
        "width": COORDINATE_SYSTEM,
        "alignment": COORDINATE_SYSTEM,
        "input_datastream_alignment": INPUT_DATASTREAM,
        "input_datastream_width": INPUT_DATASTREAM,
        "range": INPUT_DATASTREAM,
        "qc_mask": INPUT_DATASTREAM,
        "missing_value": INPUT_DATASTREAM,
        "qc_bad": INPUT_DATASTREAM,
        "std_ind_max": COORDINATE_SYSTEM,
        "std_bad_max": COORDINATE_SYSTEM,
        "goodfrac_ind_min": COORDINATE_SYSTEM,
        "goodfrac_bad_min": COORDINATE_SYSTEM,
    }

    def convert_to_adi_format(
        self, transform_parameters: Dict[Any, Any]
    ) -> Dict[str, str]:
        transforms: Dict[Any, Any] = {}
        """ 
        Example of input dictionary structure:
        
        transform_parameters = {
                "transformation_type": {
                    "time": "TRANS_AUTO"
                },
                "range": {
                    "time": 1800
                },
                "alignment": {
                    "time": LEFT
                }
        }
        """

        for parameter_name, transform_parameter in transform_parameters.items():
            parameter_type = self.transform_param_type.get(parameter_name)
            transform_parameter_name = self._get_adi_transform_parameter_name(
                parameter_name, parameter_type
            )

            # TODO: for now we are not supporting variable overrides or datastream-specific overrides.
            #   When we do, we will need to revise this syntax.  For now, the keys are the dimensions and the
            #   values are the defaults
            for dim_name, value in transform_parameter.items():
                if parameter_type == COORDINATE_SYSTEM:
                    file_name = COORDINATE_SYSTEM
                    self._write_transform_parameter_row(
                        transforms,
                        file_name,
                        None,
                        dim_name,
                        transform_parameter_name,
                        value,
                    )
                else:  # INPUT_DATASTREAM
                    file_name = INPUT_DATASTREAM
                    self._write_transform_parameter_row(
                        transforms,
                        file_name,
                        None,
                        dim_name,
                        transform_parameter_name,
                        value,
                    )

        return transforms

    def _write_transform_parameter_row(
        self,
        transforms: Dict[str, str],
        file_name: str,
        base_var_name: str,
        dim_name: str,
        parameter_name: str,
        value: str,
    ):
        # ADI transforms requires that the qc_ variable name is used instead of the actual variable name, so we need
        # to append it here
        variable_name = base_var_name
        if parameter_name == "qc_bad" or parameter_name == "qc_mask":
            if base_var_name and base_var_name[0:3] != "qc_":
                variable_name = f"qc_{base_var_name}"

        elif parameter_name == "alignment":
            value = ADIAlignments.get_adi_value(value)

        if parameter_name == "range" and value == "LENGTH_OF_PROCESSING_INTERVAL":
            # If this parameter is range and value is LENGTH_OF_PROCESSING_INTERVAL, then we can't save the parameter
            # because ADI doesn't recognize LENGTH_OF_PROCESSING_INTERVAL as a valid option.
            print(
                "Omitting range=LENGTH_OF_PROCESSING_INTERVAL since it is not recognized by ADI and is the default."
            )

        else:
            # If this is qc_mask parameter, then we have to convert the value from a binary string to integer
            if parameter_name == "qc_mask":
                value = self._convert_bit_positions_to_integer(value)

            elif parameter_name == "qc_bad":
                value = ", ".join(value)

            if file_name not in transforms:
                transforms[file_name] = ""

            row_text = f"{parameter_name} = {value};"

            # If dim is null, then it was deliberately set that way, so we should not include it in the file
            if dim_name:
                row_text = f"{dim_name}:{row_text}"
            if variable_name:
                row_text = f"{variable_name}:{row_text}"

            # Append the current row to the existing text
            existing_text = transforms[file_name]
            transforms[file_name] = f"{existing_text}{row_text}\n"

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

    def _get_adi_transform_parameter_name(self, parameter_name: str, file_type: str):
        """
        Convert transform parameter name from PCM format to names used in adi transform files.
        """
        name = parameter_name.strip().lower()

        if name == "transformation_type":
            name = "transform"  # We use a different name in our UI than in the file

        elif file_type == "input_datastream" and name == "input_datastream_alignment":
            name = "alignment"

        elif file_type == "input_datastream" and name == "input_datastream_width":
            name = "width"

        return name


class AdiTransformer:
    def transform(
        self,
        variable_name: str,
        input_dataset: xr.Dataset,
        output_dataset: xr.Dataset,
        transform_parameters: Dict[str, Any],
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
        variable_name: str
            The name of the variable being transformed.  It should have the same name in both the input and output
            datasets.
        input_dataset : xarray.Dataset
            An xarray dataset containing:
            1) A data variable to be transformed
            2) Zero or one qc_variable that contains qc flags for the data variable.  The qc_ variable must have the
                exact same base name as the data variable.  For example, if the data variable is named 'temperature',
                then the qc variable must be named qc_temperature.
                The qc_variable must not have any qc attributes set.  They will all be set by the transformer to
                specific bits that cannot be changed.
            3) One or more coordinate variables matching the coordinates on the data variable
            4) Zero or more bounds variables, one for each coordinate variable.  Bounds variables specify the front
                edge and back edge of the bins used to compute the input data values.  If no bounds variables are
                provided, ADI will assume each data point is a single, instantaneous value.  If bounds variables
                are not present in the input data files, if the user knows what the bin widths and alignments were for
                the input datastreams, they can specify these values via the width and alignment transformation
                parameters (note that these parameters are for the input datastreams, not coordinate system defaults).

                Bounds values must be the same units as their corresponding coordinate variable.  Exact values should
                be used instead of offsets.

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
                # NOTE: You do not have to set width if you provide bounds variables on the output dataset!
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

        Returns
        -------
        Void - transforms are done in-place on output_dataset
        -------------------------------------------------------------------------------------------------------------
        """

        # First convert the input and output variables into ADI format
        retrieved_dataset: cds3.Group = self._create_adi_retrieved_dataset(
            variable_name, input_dataset
        )
        transformed_dataset: cds3.Group = self._create_adi_transformed_dataset(
            variable_name, output_dataset
        )
        qc_variable_name = f"qc_{variable_name}"

        # Now convert the tranform parameters into ADI format
        adi_transform_parameters = TransformParameterConverter().convert_to_adi_format(
            transform_parameters
        )

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
        adi_input_var = (
            retrieved_dataset.get_groups()[0].get_groups()[0].get_var(variable_name)
        )
        adi_input_qc_var = (
            retrieved_dataset.get_groups()[0].get_groups()[0].get_var(qc_variable_name)
        )
        adi_output_var = (
            transformed_dataset.get_groups()[0].get_groups()[0].get_var(variable_name)
        )
        adi_output_qc_var = (
            transformed_dataset.get_groups()[0]
            .get_groups()[0]
            .get_var(qc_variable_name)
        )

        trans.transform_driver(
            adi_input_var, adi_input_qc_var, adi_output_var, adi_output_qc_var
        )

        # Now copy any changed variable attributes back to the xr out variables.
        self._update_xr_attrs(variable_name, output_dataset, transformed_dataset)

        # Now free up memory from created adi data structures
        self._free_memory(retrieved_dataset)
        self._free_memory(transformed_dataset)

    def _is_timelike(self, var: CDSVar) -> bool:
        return "time" in var.get_name()

    def _free_memory(self, adi_dataset: CDSGroup):
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
                if not self._is_timelike(var):
                    var.detach_data()

        detatch_vars(adi_dataset)

        #  After all the variable data has been detached, then we can delete the group.
        cds3.Group.delete(adi_dataset)

    def _create_adi_retrieved_dataset(
        self, variable_name: str, input_dataset: xr.Dataset
    ) -> CDSGroup:
        """-----------------------------------------------------------------------------------------------------------------
        Create the following structure in ADI:

            Dataset Group: retrieved_data
                Datastream Group: nsametC1.b1
                    Obs Group: nsametC1.b1.20140101.000000.cdf <-- dims go here
                       cds3.core.Var <-- all vars go here including coordinate vars

        Parameters
        ----------
        variable_name : str
        input_dataset : xr.Dataset

        Returns
        -------
        retrieved_dataset : cds3.Group

        -----------------------------------------------------------------------------------------------------------------
        """
        # Note:  We are not initializing datastream objects (_DSProc->datastreams) because I don't think we need it for
        # any of the libtrans operations

        # First create the dataset group
        dataset_group = cds3.Group.define(None, "retrieved_data")
        # TODO are there any global attributes that should be applied?  I don't think so, so we'll skip for now...

        # Note:  I do not think that we need to add the CDSVarGroup objects to the dataset (I couldn't see it being
        # set when I stepped through the ADI code), so we are leaving it out for now.

        # Now create the datastream group (I don't think we care what the name of the datastream is)
        datastream_group = cds3.Group.define(dataset_group, INPUT_DATASTREAM)

        # Now create the obs group
        obs_group = cds3.Group.define(datastream_group, "obs1")

        # Now add dimensions to the obs
        xr_input_var = input_dataset[variable_name]
        xr_input_qc_var = input_dataset[f"qc_{variable_name}"]
        dims = (
            xr_input_var.sizes
        )  # dict of dims and their lengths (i.e., {'time': 1440} )
        for dim_name in dims:
            # Note that we assume that time dimension will always be named 'time'
            is_unlimited = 1 if dim_name == "time" else 0
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

        # Now add the bounds transform parameters (if they apply)
        self._set_bounds_transform_parameters(variable_name, input_dataset, obs_group)

        return dataset_group

    def _create_adi_transformed_dataset(
        self, variable_name: str, output_dataset: xr.Dataset
    ) -> CDSGroup:
        """-----------------------------------------------------------------------------------------------------------------
        Create the following structure in ADI:

            Dataset Group: transformed_data
                Coordinate System Group: one_min  <-- dims go here (e.g., ['time'])
                    cds3.core.Var <-- all coord vars go here
                    Datastream Group: sbsaosmetS2.a1 <-- no dims here!
                       cds3.core.Var <-- all data vars go here (no coords!)

        Parameters
        ----------
        variable_name : str
        output_dataset : xr.Dataset

        Returns
        -------

        -----------------------------------------------------------------------------------------------------------------
        """
        # First create the dataset group
        transformed_data = cds3.Group.define(None, "transformed_data")

        # Now create the coordinate system group and add it to the transformed dataset
        cs_group = cds3.Group.define(transformed_data, COORDINATE_SYSTEM)

        # Now add the dimensions to the coordinate system group
        xr_output_var = output_dataset[variable_name]
        dims = (
            xr_output_var.sizes
        )  # dict of dims and their lengths (i.e., {'time': 1440} )
        for dim_name in dims:
            # Note that we assume that time dimension will always be named 'time'
            # is_unlimited = 1 if dim_name != "time" else 0
            # --> fails for 2D variables if set to "!=", fails for 1D if set to ""=="
            is_unlimited = 0
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
        xr_output_qc_var = output_dataset[f"qc_{variable_name}"]
        self._add_variable_to_adi(
            xr_output_var, ds_group, coordinate_system_name=COORDINATE_SYSTEM
        )
        self._add_variable_to_adi(
            xr_output_qc_var, ds_group, coordinate_system_name=COORDINATE_SYSTEM
        )

        # Now add the bounds transform parameters (if they apply)
        self._set_bounds_transform_parameters(variable_name, output_dataset, cs_group)

        return transformed_data

    def _update_xr_attrs(
        self,
        variable_name: str,
        output_dataset: xr.Dataset,
        transformed_dataset: CDSGroup,
    ):
        # Sync the transform attributes back to the xarray variable and qc_variable
        adi_var = (
            transformed_dataset.get_groups()[0].get_groups()[0].get_var(variable_name)
        )
        xr_var = output_dataset.get(variable_name)

        # First copy over any attributes that were set on the adi data variable back to xarray:
        adi_atts: List[cds3.Att] = adi_var.get_atts()
        xr_attrs = {
            att.get_name(): dsproc.get_att_value(
                adi_var, att.get_name(), att.get_type()
            )
            for att in adi_atts
        }
        for name, value in xr_attrs.items():
            if name not in xr_var.attrs:
                xr_var.attrs[name] = value

        # Now set all of the qc attributes
        qc_var = output_dataset.get(f"qc_{variable_name}")
        xr_attrs = {
            "units": 1,
            "long_name": f"Quality check results on variable: {variable_name}",
        }
        xr_attrs.update(self._back_convert_qc_atts(adi_qc_atts))
        for name, value in xr_attrs.items():
            if name not in qc_var.attrs:
                qc_var.attrs[name] = value

    def _add_atts_to_adi(self, xr_var: xr.DataArray, adi_obj: CDSObject):
        encoding_atts = {
            att: xr_var.encoding[att]
            for att in ["_FillValue", "source", "units"]
            if att in xr_var.encoding
        }
        xr_atts_dict = {**encoding_atts, **xr_var.attrs}
        atts = xr_atts_dict

        # If this is a qc variable, then we need to convert over the qc attributes
        if adi_obj.get_name().startswith("qc_"):
            # Use a new dictionary of atts so we don't mess with the attributes for the original data array
            atts = {}
            qc_atts = {}

            # Collect the qc atts together, since we need to convert them together
            for att_name, att_value in xr_atts_dict.items():
                if (
                    att_name == "flag_masks"
                    or att_name == "flag_meanings"
                    or att_name == "flag_assessments"
                ):
                    qc_atts[att_name] = att_value
                else:
                    atts[att_name] = att_value

            # Convert the qc atts to ADI format (Tsdat uses the condensed ACT format)
            atts.update(self._convert_qc_atts(qc_atts))

            # We also have to add the flag_method att
            atts["flag_method"] = "bit"

        # Now add the atts to ADI
        for att_name, att_value in atts.items():
            cds_type = self._get_cds_type(att_value)
            status = dsproc.set_att(adi_obj, 1, att_name, cds_type, att_value)
            if status < 1:
                raise Exception(f"Could not create attribute {att_name}")

    def _back_convert_qc_atts(self, adi_qc_atts: Dict) -> Dict:
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
        -------------------------------------------------------------------------------------------------------------
        """
        bit_metadata = {}
        bit_pattern = re.compile(r"^bit_(\d+)_(.+)$")

        # First we build up a dictionary sorted by bit number
        for att_name, att_value in adi_qc_atts.items():
            match = bit_pattern.match(att_name)
            if match:
                bit_number = int(match.groups()[0])
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
            power = bit_number - 1
            mask = int(math.pow(2, power))
            flag_masks.append(mask)
            flag_meanings.append(metadata.get("description"))
            flag_assessments.append(metadata.get("assessment"))

        xr_qc_atts = {
            "flag_masks": flag_masks,
            "flag_meanings": flag_meanings,
            "flag_assessments": flag_assessments,
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
        -------------------------------------------------------------------------------------------------------------
        """

        flag_masks = xr_qc_atts.get("flag_masks", [])
        flag_meanings = xr_qc_atts.get("flag_meanings", [])
        flag_assessments = xr_qc_atts.get("flag_assessments", [])

        # Note that I think ADI may crash if the bit numbers are not contiguous
        adi_qc_atts = {}
        for i in range(len(flag_masks)):
            # Convert the integer to a bit position starting from 1
            bit_number = str(int(math.log2(flag_masks[i])) + 1)
            adi_qc_atts[f"bit_{bit_number}_description"] = flag_meanings[i]
            adi_qc_atts[f"bit_{bit_number}_assessment"] = flag_assessments[i]

        return adi_qc_atts

    def _add_variable_to_adi(
        self,
        xr_var: xr.DataArray,
        parent_group: CDSGroup,
        coordinate_system_name: str = None,
    ):
        """-----------------------------------------------------------------------------------------------------------------
        Add a variable specified by an xarray DataArray to the given ADI dataset.

        TODO: Do we need to add any VarTags?  I don't think so, since they are only used by dsproc, not libtrans.
            Vartags are:  source_ds_name, source_var_name, output_targets, and coordinate_system
        -----------------------------------------------------------------------------------------------------------------
        """
        # First create the variable
        cds_type = self._get_cds_type(xr_var.data)
        dim_names = xr_dims = list(xr_var.dims)
        adi_var = dsproc.define_var(parent_group, xr_var.name, cds_type, dim_names)

        # Now assign attributes
        self._add_atts_to_adi(xr_var, adi_var)

        # Now set the variable's data
        if np.issubdtype(xr_var.dtype, np.datetime64):
            # If this is time, then we have to convert the values because xarray time is different
            self._set_time_variable_data(xr_var, adi_var)
        else:
            # Just use the same data pointer to the numpy ndarray
            sample_count = xr_var.sizes[dim_names[0]]
            adi_var.attach_data(
                xr_var.data.__array_interface__["data"][0], sample_count
            )

        # Add the coordinate system name to a VarTag object for the variable (not sure if we need this for transform)
        if coordinate_system_name:
            dsproc.set_var_coordsys_name(adi_var, coordinate_system_name)

    def _set_time_variable_data(self, xr_var: xr.DataArray, adi_var: CDSVar):
        """-----------------------------------------------------------------------------------------------------------------
        For time values, we actually have to create a copy.  We can't rely on the data pointer for time, because the times
        are converted into datetime64 objects for xarray.
        -----------------------------------------------------------------------------------------------------------------
        """
        # Convert numpy datetime64 to seconds
        timevals = self._convert_time_data(xr_var)

        # Set the timevals in seconds in ADI
        dsproc.set_sample_timevals(adi_var, 0, timevals)

    def _convert_time_data(self, xr_array: xr.DataArray) -> np.ndarray:
        # astype will produce nanosecond precision, so we have to convert to seconds
        timevals = xr_array.data.astype("float") / 1000000000

        # We have to truncate to 6 decimal places so it matches ADI
        timevals = np.around(timevals, 6)

        return timevals

    def _convert_non_time_bounds_data(self, xr_array: xr.DataArray) -> np.ndarray:
        return xr_array.data.astype("float")

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

    def _set_bounds_transform_parameters(
        self, variable_name: str, xr_dataset: xr.Dataset, obs_or_coord_group: CDSGroup
    ):
        # Get the bounds variable for each dimension used by our variable.  If no bounds variable exists, then skip.
        # Bounds variable saves the offset for each data point instead of full value
        for dim in xr_dataset[variable_name].dims:
            bounds_var: xr.DataArray = xr_dataset.get(f"{dim}_bounds")

            if bounds_var is not None:
                front_edge = bounds_var.T[
                    0
                ]  # Front edge is the first column of bounds var
                back_edge = bounds_var.T[
                    1
                ]  # Back edge is the second column of bounds var

                # We have to make sure that the data are converted to floats to set the transform parameter properly
                front_data: np.ndarray
                back_data: np.ndarray
                if np.issubdtype(bounds_var.dtype, np.datetime64):
                    front_data = self._convert_time_data(front_edge)
                    back_data = self._convert_time_data(back_edge)
                else:
                    front_data = self._convert_non_time_bounds_data(front_edge)
                    back_data = self._convert_non_time_bounds_data(back_edge)

                cds3.set_front_edge_param(
                    obs_or_coord_group, dim, front_edge.size, front_data
                )
                cds3.set_back_edge_param(
                    obs_or_coord_group, dim, back_edge.size, back_data
                )
