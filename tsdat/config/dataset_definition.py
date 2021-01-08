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

    def _parse_attributes(self, dict: Dict) -> List[AttributeDefinition]:
        atts_dict = dict.get(Keys.ATTRIBUTE, [])
        pass
    
    def _parse_dimensions(self, dict: Dict) -> List[DimensionDefinition]:
        dims_dict = dict.get(Keys.DIMENSIONS, {})
        pass

    def _parse_variables(self, dict: Dict) -> List[VariableDefinition]:
        pass
    
    def to_dict(self) -> Dict:
        # Returns a dictionary that can be used to instantiate
        # an xarray Dataset with no data
        pass
