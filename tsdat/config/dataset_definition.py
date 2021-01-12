from typing import Dict, List
from .keys import Keys
from .attribute_defintion import AttributeDefinition
from .dimension_definition import DimensionDefinition
from .variable_definition import VariableDefinition

class DatasetDefinition:
    def __init__(self, dictionary: Dict):
        self.attributes = self._parse_attributes(dictionary)
        self.dimensions = self._parse_dimensions(dictionary)
        self.variables = self._parse_variables(dictionary)

    def _parse_attributes(self, dictionary: Dict) -> Dict[str, AttributeDefinition]:
        attributes: Dict[str, AttributeDefinition] = {}
        for attr_name, attr_value in dictionary[Keys.ATTRIBUTES].items():
            attributes[attr_name] = AttributeDefinition(attr_name, attr_value)
        return attributes
    
    def _parse_dimensions(self, dictionary: Dict) -> Dict[str, DimensionDefinition]:
        dimensions: Dict[str, DimensionDefinition] = {}
        for dim_name, dim_dict in dictionary[Keys.DIMENSIONS].items():
            dimensions[dim_name] = DimensionDefinition(dim_name, dim_dict)
        return dimensions

    def _parse_variables(self, dictionary: Dict) -> Dict[str, VariableDefinition]:
        variables: Dict[str, VariableDefinition] = {}
        for var_name, var_dict in dictionary[Keys.VARIABLES].items():
            variables[var_name] = VariableDefinition(var_name, var_dict)
        return variables
    
    # def get_variable_names(self):
    #     # Stupid python 3 returns keys as a dict_keys object.
    #     # Not really sure the purpose of this extra class :(.
    #     return list(self.variables.keys())

    # def get_variable(self, variable_name):
    #     return self.variables.get(variable_name, None)

    # def get_variables(self):
    #     return self.variables.values()

    def to_dict(self) -> Dict:
        """-------------------------------------------------------------------
        Returns a dictionary that can be used to instatiate an xarray dataset 
        with no data.

        Returns:
            Dict: A dictionary representing the structure of the dataset.
        -------------------------------------------------------------------"""
        dictionary = {
            "attributes": self.attributes.to_dict(),
            "dimensions": self.dimensions.to_dict(),
            "variables": self.variables.to_dict()
        }
        return dictionary
