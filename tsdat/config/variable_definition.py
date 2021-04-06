import numpy as np
from typing import Any, Dict, List, Tuple
from .dimension_definition import DimensionDefinition
from .utils import instantiate_handler
from tsdat.utils.converters import Converter


class VarKeys:
    INPUT = 'input'
    DIMS = 'dims'
    TYPE = 'type'
    ATTRS = 'attrs'


class VarInputKeys:
    NAME = 'name'
    CONVERTER = 'converter'
    # TIME_FORMAT = 'time_format'
    # TIMEZONE = 'timezone'
    UNITS = 'units'
    REQUIRED = 'required'

class ConverterKeys:
    CLASSNAME = 'classname'
    PARAMETERS = 'parameters'


class VarInput:    
    """-----------------------------------------------------------------------
    Class to explicitly encode fields set by the variable's input source 
    defined by the yaml file.
    -----------------------------------------------------------------------"""
    def __init__(self, dictionary: Dict, defaults: Dict = {}):
        
        # Name will always come from input dictionary and is required
        self.name: str = dictionary.get(VarInputKeys.NAME)
        
        # Set the converter defaults if no converter is provided
        classname = "tsdat.utils.converters.DefaultConverter"
        parameters = {}

        # Update classname and add parameters from defaults
        _default_converter = defaults.get(VarInputKeys.CONVERTER, {})
        classname = _default_converter.get(ConverterKeys.CLASSNAME, classname)
        parameters.update(_default_converter.get(ConverterKeys.PARAMETERS, {}))

        # Update classname and add parameters from dictionary input
        _provided_converter = dictionary.get(VarInputKeys.CONVERTER, {})
        classname = _provided_converter.get(ConverterKeys.CLASSNAME, classname)
        parameters.update(_provided_converter.get(ConverterKeys.PARAMETERS, {}))
        
        # Instantiate the converter
        converter = {ConverterKeys.CLASSNAME: classname, ConverterKeys.PARAMETERS: parameters}
        self.converter: Converter = instantiate_handler(handler_desc=converter)

        # Add any other input properties to the variable input
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])
        
        for key in defaults:
            if not hasattr(self, key):
                setattr(self, key, defaults[key])
    
    def is_required(self) -> bool:
        return getattr(self, VarInputKeys.REQUIRED, False) == True


class VariableDefinition:
    """-----------------------------------------------------------------------
    Class to encode variable definitions from the config file. Also provides
    a few utility methods.
    -----------------------------------------------------------------------"""
    def __init__(self, name: str, dictionary: Dict, available_dimensions: Dict[str, DimensionDefinition], defaults: Dict = {}):
        self.name: str = name
        self.input = self._parse_input(dictionary, defaults)
        self.dims = self._parse_dimensions(dictionary, available_dimensions, defaults)
        self.attrs = self._parse_attributes(dictionary, defaults)
        self.type = self._parse_data_type(dictionary, defaults)

        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])

        self._predefined = hasattr(self, "data")
    
    def _parse_input(self, dictionary: Dict, defaults: Dict = {}) -> VarInput:
        input_source = dictionary.get(VarKeys.INPUT, None)
        if not input_source:
            return None
        return VarInput(input_source, defaults.get(VarKeys.INPUT, {}))
    
    def _parse_attributes(self, dictionary: Dict, defaults: Dict = {}) -> Dict[str, Any]:
        # Initialize attributes dictionary. Defaults used only for non-coordinate variables
        attributes: Dict[str, Any] = {}
        if not self.is_coordinate():
            attributes = self.add_fillvalue_if_none(attributes)
            attributes.update(defaults.get(VarKeys.ATTRS, {}))
        
        # Add attributes from the variable's definition and return. This overwrites 
        # any conflicting attributes with the values from dictionary
        attributes.update(dictionary.get(VarKeys.ATTRS, {}))
        return attributes
        
    def _parse_dimensions(self, dictionary: Dict, available_dimensions: Dict[str, DimensionDefinition], defaults: Dict = {}) -> Dict[str, DimensionDefinition]:
        default_dims: List[str] = defaults.get(VarKeys.DIMS, [])
        requested_dimensions: List[str] = dictionary.get(VarKeys.DIMS, default_dims)
        parsed_dimensions: Dict[str, DimensionDefinition] = {}
        for dim in requested_dimensions:
            if dim not in available_dimensions:
                message = f"Dimension '{dim}' for variable '{self.name}' has not been defined in the config file."
                raise KeyError(message)
            parsed_dimensions[dim] = available_dimensions[dim]
        return parsed_dimensions

    def _parse_data_type(self, dictionary: Dict, defaults: Dict = {}) -> object:
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
        default_data_type = defaults.get(VarKeys.TYPE, None)
        data_type: str = dictionary.get(VarKeys.TYPE, default_data_type)
        mappings = {
            "str":      str,
            "char":     str,
            "byte":     np.int8,
            "int8":     np.int8,
            "ubyte":    np.uint8,
            "uint8":    np.uint8,
            "short":    np.int16,
            "int16":    np.int16,
            "ushort":   np.uint16,
            "uint16":   np.uint16,
            "int":      np.int32,
            "uint":     np.int32,
            "int32":    np.int32,
            "uint32":   np.uint32,
            "int64":    np.int64,
            "uint64":   np.uint64,
            "long":     np.int64,
            "ulong":    np.uint64,
            "float":    np.float32,
            "float32":  np.float32,
            "double":   np.float64,
            "float64":  np.float64
        }
        if data_type not in mappings:
            message = f"Invalid data type on variable {self.name}: '{data_type}'. "
            message += f"Data type must be one of: \n{', '.join(list(mappings.keys()))}"
            raise KeyError(message)
        return mappings[data_type]

    def add_fillvalue_if_none(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """-------------------------------------------------------------------
        Adds the _FillValue attribute to the provided attributes dictionary if
        the _FillValue attribute has not already been defined and returns the 
        modified attributes dictionary.

        Args:
            attributes (Dict[str, Any]):    The dictionary containing user- 
                                            defined variable attributes.

        Returns:
            Dict[str, Any]: The dictionary containing user-defined variable 
                            attributes. Is guaranteed to have a _FillValue 
                            attribute.
        -------------------------------------------------------------------"""
        current_attr = attributes.get("_FillValue", -9999)
        attributes["_FillValue"] = current_attr
        return attributes

    def is_constant(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the variable is a constant. A variable is constant if
        it does not have any dimensions.

        Returns:
            bool: True if the variable is constant, False otherwise.
        -------------------------------------------------------------------"""
        return len(list(self.dims.keys())) == 0
    
    def is_predefined(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the variable's data was predefined in the config yaml 
        file.

        Returns:
            bool: True if the variable is predefined, False otherwise.
        -------------------------------------------------------------------"""
        return self._predefined

    def is_coordinate(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the variable is a coordinate variable. A variable is a 
        coordinate variable if it is dimensioned by itself.

        Returns:
            bool:   True if the variable is a coordinate variable, False 
                    otherwise.
        -------------------------------------------------------------------"""
        return [self.name] == [dim_name for dim_name in self.dims]


    def is_derived(self) -> bool:
        """-------------------------------------------------------------------
        Return True if the variable is derived. A variable is derived if it 
        does not have an input and it is not predefined.

        Returns:
            bool: True if the Variable is derived, False otherwise.
        -------------------------------------------------------------------"""
        return self.input is None and not self.is_predefined()
    
    def has_converter(self) -> bool:
        """-------------------------------------------------------------------
        Return True if the variable has an input converter defined, False
        otherwise.

        Returns:
            bool:   True if the Variable has a converter defined, False 
                    otherwise.
        -------------------------------------------------------------------"""
        return self.has_input() and hasattr(self.input, "converter")

    def is_required(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the variable has the 'required' property defined and 
        the 'required' property evaluates to True. A required variable is a 
        variable which much be retrieved in the input dataset. If a required
        variable is not in the input dataset, the process should crash.

        Returns:
            bool: True if the Variable is required, False otherwise.
        -------------------------------------------------------------------"""
        return self.has_input() and self.input.is_required()

    def has_input(self) -> bool:
        """-------------------------------------------------------------------
        Return True if the variable is copied from an input dataset, 
        regardless of whether or not unit and/or naming conversions should be 
        applied.

        Returns:
            bool: True if the Variable has an input defined, False otherwise.
        -------------------------------------------------------------------"""
        return self.input is not None

    def get_input_name(self) -> str:
        """-------------------------------------------------------------------
        Returns the name of the variable in the input if defined, otherwise 
        returns None.

        Returns:
            str: The name of the variable in the input, or None.
        -------------------------------------------------------------------"""
        if not self.has_input():
            return None
        return self.input.name
    
    def get_input_units(self) -> str:
        """-------------------------------------------------------------------
        If the variable has input, returns the units of the input variable or 
        the output units if no input units are defined.

        Returns:
            str: The units of the input variable data.
        -------------------------------------------------------------------"""
        if not self.has_input():
            return None
        return getattr(self.input, "units", self.get_output_units())
    
    def get_output_units(self) -> str:
        """-------------------------------------------------------------------
        Returns the units of the output data or None if no units attribute has
        been defined.

        Returns:
            str: The units of the output variable data.
        -------------------------------------------------------------------"""
        return self.attrs.get("units", None)

    def get_coordinate_names(self) -> List[str]:
        """-------------------------------------------------------------------
        Returns the names of the coordinate VariableDefinition(s) that this 
        VariableDefinition is dimensioned by.

        Returns:
            List[str]: A list of dimension/coordinate variable names.
        -------------------------------------------------------------------"""
        return list(self.dims.keys())
    
    def get_shape(self) -> Tuple[int]:
        """-------------------------------------------------------------------
        Returns the shape of the data attribute on the VariableDefinition. 
        Raises a KeyError if the data attribute has not been set yet.

        Returns:
            Tuple[int]: The shape of the VariableDefinition's data, or None.
        -------------------------------------------------------------------"""
        if not hasattr(self, "data"):
            raise KeyError(f"No data has been set for variable: '{self.name}'")
        return self.data.shape

    def get_data_type(self) -> np.dtype:
        """-------------------------------------------------------------------
        Returns the data type of the variable's data as a numpy dtype.
        -------------------------------------------------------------------"""
        return self.type
    
    def get_FillValue(self):
        """-------------------------------------------------------------------
        Returns the variable's _FillValue attribute, using -9999 as a default
        if the _FillValue attribute has not been defined.
        -------------------------------------------------------------------"""
        return getattr(self.attrs.get("_FillValue", -9999), "value", -9999)

    def run_converter(self, data: np.ndarray) -> np.ndarray:
        """-------------------------------------------------------------------
        If the variable has an input converter, runs the input converter for 
        the input/output units on the provided data.

        Args:
            data (np.ndarray): The data to be converted.
        
        Returns:
            np.ndarray: The data, converted to its output units.
        -------------------------------------------------------------------"""
        if self.has_converter():
            in_units = self.get_input_units()
            out_units = self.get_output_units()
            return self.input.converter.run(data, in_units, out_units)
        return data


    def to_dict(self) -> Dict:
        """-------------------------------------------------------------------
        Returns the Variable as a dictionary to be used to intialize an xarray
        Dataset or DataArray.

        Returns a dictionary like (Example is for `temperature`):
        ```
        {
            "dims": ["time"], 
            "data": [], 
            "attrs": {"units": "degC"}
        }
        ```

        Returns:
            Dict: A dictionary representation of the variable.
        -------------------------------------------------------------------"""
        dictionary = {
            "dims":     [name for name, dim in self.dims.items()],
            "data":     self.data if hasattr(self, "data") else [],
            "attrs":    self.attrs
        }
        return dictionary
        