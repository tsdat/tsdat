import numpy as np
from typing import Dict, List
from .attribute_defintion import AttributeDefinition
from .dimension_definition import DimensionDefinition

class VarKeys:
    INPUT = 'input'
    DIMS = 'dims'
    TYPE = 'type'
    ATTRS = 'attrs'

class VariableDefinition:
    def __init__(self, name: str, dictionary: Dict):
        self.name = name
        self.input = dict.get("input", {})
        self.attrs = dict.get('attrs', {})
        self.dims = dict.get("dims", [])
        self.type = dict.get("type", None)
        for key in dict:
            if not hasattr(self, key):
                setattr(self, key, dict[key])
    
    def _parse_data_type(self, data_type: str):
        """-------------------------------------------------------------------
        Parses the data_type string and returns the appropriate numpy data 
        type (i.e. "float" -> np.float). 

        Args:
            data_type (str): the data type as read from the yaml.

        Returns:
            Object: The numpy data type corresponding with the type provided
                    in the yaml file, or data_type if the provided data_type
                    is not in the MHKiT-Cloud Data Standards list of data 
                    types.
        -------------------------------------------------------------------"""
        mappings = {
            "string":   np.str,
            "char":     np.str,
            "byte":     np.int8,
            "ubyte":    np.uint8,
            "short":    np.int16,
            "ushort":   np.uint16,
            "long":     np.int64,
            "ulong":    np.uint64,
            "int":      np.int32,
            "float":    np.float32,
            "double":   np.float64
        }
        if data_type not in mappings:
            error_message = f"{data_type} is not a standard data type. Data type must be one of: \n{'\n'.join(list(mappings.keys()))}"
            raise KeyError(error_message)
        return mappings[data_type]

    def _parse_dimensions(self, dictionary: Dict) -> List[str]:
        return dictionary.get("dims", [])

    def is_constant(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the variable is a constant. A variable is constant if
        it does not have any dimensions.

        Returns:
            bool: True if the variable is constant, False otherwise.
        -------------------------------------------------------------------"""
        return len(self.dims) == 0

    def is_coordinate(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the variable is a coordinate variable. A variable is a 
        coordinate variable if it is dimensioned by itself.

        Returns:
            bool:   True if the variable is a coordinate variable, False 
                    otherwise.
        -------------------------------------------------------------------"""
        return self.name in [dim.name for dim in self.dims]

    def is_derived(self) -> bool:
        """Return True if the variable is derived. A variable is derived if it does not have an input.

        Returns:
            bool: True if the Variable is derived, False otherwise.
        """
        return self.input == {}

    def get_units(self):
        # TODO: return units as cfunits object
        return self.attrs.get("units", 1)
