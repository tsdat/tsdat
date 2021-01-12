from typing import Dict, List, Tuple
from .keys import Keys
from .attribute_defintion import AttributeDefinition
from .dimension_definition import DimensionDefinition
from .variable_definition import VariableDefinition

class DatasetDefinition:
    def __init__(self, dictionary: Dict):
        self.attrs = self._parse_attributes(dictionary)
        self.dims = self._parse_dimensions(dictionary)
        self.vars = self._parse_variables(dictionary, self.dims)
        self.coords, self.vars = self._parse_coordinates(self.vars)

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

    def get_variable(self, variable_name) -> VariableDefinition:
        return self.variables.get(variable_name, None)

    def get_variables(self) -> List[VariableDefinition]:
        return self.variables.values()

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
