import act
import numpy as np
import xarray as xr
from typing import Dict, List, Tuple
from .keys import Keys
from .attribute_defintion import AttributeDefinition
from .dimension_definition import DimensionDefinition
from .variable_definition import VariableDefinition

class DatasetDefinition:
    def __init__(self, dictionary: Dict):
        self._parse_required_attributes(dictionary.get(Keys.ATTRIBUTES))
        self.attrs = self._parse_attributes(dictionary)
        self.dims = self._parse_dimensions(dictionary)
        self.vars = self._parse_variables(dictionary, self.dims)
        self.coords, self.vars = self._parse_coordinates(self.vars)

    def _parse_required_attributes(self, dictionary: Dict):
        """-------------------------------------------------------------------
        Creates handles for several required attributes that must be set 
        before runtime.

        Args:
            dictionary (Dict): The dictionary containing global attributes.
        -------------------------------------------------------------------"""
        self.title: str             = dictionary.get("title")
        self.description: str       = dictionary.get("description")
        self.conventions: str       = dictionary.get("conventions")
        self.code_url: str          = dictionary.get("code_url")
        self.input_files: List[str] = dictionary.get("input_files")
        self.location_id: str       = dictionary.get("location_id")
        self.datastream: str        = dictionary.get("datastream")
        self.data_level: str        = dictionary.get("data_level")

    def _parse_attributes(self, dictionary: Dict) -> Dict[str, AttributeDefinition]:
        attributes: Dict[str, AttributeDefinition] = {}
        for attr_name, attr_value in dictionary.get(Keys.ATTRIBUTES, {}).items():
            attributes[attr_name] = AttributeDefinition(attr_name, attr_value)
        return attributes
    
    def _parse_dimensions(self, dictionary: Dict) -> Dict[str, DimensionDefinition]:
        dimensions: Dict[str, DimensionDefinition] = {}
        for dim_name, dim_dict in dictionary[Keys.DIMENSIONS].items():
            dimensions[dim_name] = DimensionDefinition(dim_name, dim_dict)
        return dimensions

    def _parse_variables(self, dictionary: Dict, available_dimensions: Dict[str, DimensionDefinition]) -> Dict[str, VariableDefinition]:
        variables: Dict[str, VariableDefinition] = {}
        for var_name, var_dict in dictionary[Keys.VARIABLES].items():
            variables[var_name] = VariableDefinition(var_name, var_dict, available_dimensions)
        return variables
    
    def _parse_coordinates(self, vars: Dict[str, VariableDefinition]) -> Tuple[Dict[str, VariableDefinition], Dict[str, VariableDefinition]]:
        """-------------------------------------------------------------------
        Determines which variables are coordinate variables and moves those 
        variables from self.vars to self.coords. Coordinate variables are 
        variables that are dimensioned by themself. I.e. `var.name == 
        var.dim.name` is a true statement for coordinate variables.

        Args:
            vars (Dict[str, VariableDefinition]):   The dictionary of 
                                                    variables to check.
            dims (Dict[str, DimensionDefinition]):  The dictionary of 
                                                    dimensions in the dataset.
        -------------------------------------------------------------------"""
        coords = {name: var for name, var in vars.items() if var.is_coordinate()}
        vars = {name: var for name, var in vars.items() if not var.is_coordinate()}
        return coords, vars
    
    def get_variable_names(self) -> List[str]:
        return list(self.variables.keys())

    def get_variable(self, variable_name: str) -> VariableDefinition:
        return self.variables.get(variable_name, None)

    def get_variables(self, variable_names: List[str]) -> List[VariableDefinition]:
        return [self.get_variable(var_name) for var_name in variable_names]
    
    def get_coordinates(self, variable: VariableDefinition) -> List[VariableDefinition]:
        """-------------------------------------------------------------------
        Returns the coordinate VariableDefinition(s) that dimension the 
        provided variable.

        Args:
            variable (VariableDefinition):  The VariableDefinition whose 
                                            coordinate variables should be 
                                            retrieved.

        Returns:
            List[VariableDefinition]:   A list of VariableDefinition 
                                        coordinate variables that dimension
                                        the given VariableDefinition.
        """
        coordinate_names = variable.get_coordinate_names()
        return [self.coords.get(coord_name) for coord_name in coordinate_names]

    def get_variable_shape(self, variable: VariableDefinition) -> Tuple[int]:
        coordinates = self.get_coordinates(variable)
        shape = tuple([coord.get_shape()[0] for coord in coordinates])
        return shape

    def extract_data(self, variable: VariableDefinition, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Adds data from the xarray dataset to the given VariableDefinition. It 
        can convert units and use _FillValue to initilize variables not taken 
        from the dataset.

        Args:
            variable (VariableDefinition): The VariableDefinition to update.
            dataset (xr.Dataset): The dataset to draw data from.
        -------------------------------------------------------------------"""
        # If variable is predefined, it should already have the appropriate 
        # represention in the definition; do nothing.
        if variable.is_predefined():
            dtype = variable.get_data_type()
            variable.data = np.array(variable.data, dtype=dtype)
        
        # If variable has no input, retrieve its _FillValue and shape, then 
        # initialize the data in the VariableDefinition.
        if variable.is_derived():
            if variable.is_coordinate():
                raise Exception("Error: coordinate variable {variable.name} must not be empty")
            shape = self.get_variable_shape(variable)
            _FillValue = variable.get_FillValue()
            dtype = variable.get_data_type()
            variable.data = np.full(shape, _FillValue, dtype=dtype)
        
        # If variable has input and is in the dataset, perform sanity checks
        # then convert units and add it to the VariableDefinition
        if variable.has_input():
            input_name = variable.get_input_name()
            data = dataset[input_name].values
            converted = variable.input.converter.run(data, variable.get_input_units(), variable.get_output_units())
            variable.data = converted

    def to_dict(self) -> Dict:
        """-------------------------------------------------------------------
        Returns a dictionary that can be used to instantiate an xarray dataset 
        with no data.

        Returns a dictionary like:
        ```
        {
            "coords": {"time": {"dims": ["time"], "data": [], "attrs": {"units": "seconds since 1970-01-01T00:00:00"}}},
            "attrs": {"title": "Ocean Temperature and Salinity"},
            "dims": "time",
            "data_vars": {
                "temperature": {"dims": ["time"], "data": [], "attrs": {"units": "degC"}},
                "salinity": {"dims": ["time"], "data": [], "attrs": {"units": "kg/m^3"}},
            },
        }
        ```

        Returns:
            Dict: A dictionary representing the structure of the dataset.
        -------------------------------------------------------------------"""
        dictionary = {
            "coords":       {coord_name: coord.to_dict() for coord_name, coord in self.coords.items()},
            "attrs":        {attr_name: attr.value for attr_name, attr in self.attrs.items()},
            "dims":         list(self.dims.keys()),
            "data_vars":    {var_name: var.to_dict() for var_name, var in self.vars.items()}
        }
        return dictionary
