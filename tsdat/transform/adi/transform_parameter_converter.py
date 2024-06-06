from typing import Any, Dict, Optional
import math
import numpy as np

from .adi_alignments import ADIAlignments
from ...const import COORDINATE_SYSTEM, INPUT_DATASTREAM


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
        base_var_name: Optional[str],
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
                "Omitting range=LENGTH_OF_PROCESSING_INTERVAL since it is not"
                " recognized by ADI and is the default."
            )

        elif (
            parameter_name in ["range", "width"]
            and not isinstance(value, int)
            and value[-1] != "s"
        ):
            seconds = np.timedelta64(value[:-1], value[-1]).item().total_seconds()
            value = str(int(seconds)) + "s"

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

    @staticmethod
    def _convert_bit_positions_to_integer(bit_position_array):
        """
        Convert an array of bit positions starting at bit 1 for the zeroeth bit (ie., [1,3]) into an
        integer with the proper bits flipped.
        """
        int_value = 0
        for bit_position in bit_position_array:  # ie., [1,3]
            power = int(bit_position) - 1
            int_value += int(math.pow(2, power))

        return int_value

    @staticmethod
    def _get_adi_transform_parameter_name(parameter_name: str, file_type: str):
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
