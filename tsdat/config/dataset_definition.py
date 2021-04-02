import act
import datetime
import warnings
import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Any
from .keys import Keys
from .dimension_definition import DimensionDefinition
from .variable_definition import VariableDefinition
from tsdat.exceptions import DefinitionError


class DatasetDefinition:
    def __init__(self, dictionary: Dict, datastream_name: str):
        # Add global attributes
        self.attrs = dictionary.get(Keys.ATTRIBUTES, {})
        self.attrs["datastream_name"] = datastream_name

        # Parse dimensions
        self.dims = self._parse_dimensions(dictionary)

        # Parse variables and coordinate variables
        self.vars = self._parse_variables(dictionary, self.dims)
        self.coords, self.vars = self._parse_coordinates(self.vars)

        # Validate the dataset
        self._validate_dataset_definition()

    def _parse_dimensions(self, dictionary: Dict) -> Dict[str, DimensionDefinition]:
        dimensions: Dict[str, DimensionDefinition] = {}
        for dim_name, dim_dict in dictionary[Keys.DIMENSIONS].items():
            dimensions[dim_name] = DimensionDefinition(dim_name, dim_dict)
        return dimensions

    def _parse_variables(self, dictionary: Dict, available_dimensions: Dict[str, DimensionDefinition]) -> Dict[str, VariableDefinition]:
        defaults: Dict[str, Any] = dictionary.get(Keys.DEFAULTS, {})
        variables: Dict[str, VariableDefinition] = {}
        for var_name, var_dict in dictionary[Keys.VARIABLES].items():
            variables[var_name] = VariableDefinition(var_name, var_dict, available_dimensions, defaults=defaults)
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

    def _generate_history(self, dictionary: Dict) -> str:
        # Should generate a string like: "Ran by user <USER> on machine <MACHINE> at <DATE>"
        # TODO: Add user
        # TODO: Add machine, if possible
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"Ran at {date}"
    
    def _validate_dataset_definition(self):
        """-------------------------------------------------------------------
        Performs sanity checks on the dataset definition after it has been 
        parsed from the yaml file.

        Raises:
            DefinitionError: Raises a DefinitionError if a sanity check fails.
        -------------------------------------------------------------------"""
        # Ensure that there is a time coordinate variable
        if "time" not in self.coords:
            raise DefinitionError("'time' must be defined as a coordinate variable.")
        
        # Warn if any dimensions do not have an associated coordinate variable
        dims_without_coords = [dim for dim in self.dims if dim not in self.coords]
        for dim in dims_without_coords:
            warnings.warn(f"Dimension {dim} does not have an associated coordinate variable.")

        # Ensure that all coordinate variables are dimensioned by themselves
        valid = lambda coord: (list(coord.dims.keys()) == [coord.name])
        bad_coordinates = [coord.name for coord in self.coords.values() if not valid(coord)]
        if bad_coordinates:
            raise DefinitionError(f"The following coordinate variable(s) are not dimensioned solely by themselves:\n{bad_coordinates}")

        # TODO: Check Variable attributes -- _FillValue defined only on non-coordinate variables,
        # warning if no long_name provided, units recognized by our units library, etc
    
    def get_attr(self, attribute_name):
        return self.attrs.get(attribute_name, None)

    def get_variable_names(self) -> List[str]:
        return list(self.vars.keys())

    def get_variable(self, variable_name: str) -> VariableDefinition:
        variable = self.vars.get(variable_name, None)
        if variable is None:
            variable = self.coords.get(variable_name, None)
        return variable
    
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
        -------------------------------------------------------------------"""
        coordinate_names = variable.get_coordinate_names()
        return [self.coords.get(coord_name) for coord_name in coordinate_names]

    def get_static_variables(self) -> List[VariableDefinition]:
        """-------------------------------------------------------------------
        Returns a list of static VariableDefinitions. A VariableDefinition is 
        "static" if it has "data" section in the config file. For example, 
        "depth" as defined below is a static variable:
        
        ```
        depth:
          data: [4, 8, 12]
          dims: [depth]
          type: long
          attrs:
            long_name: Depth
            units: m
        ```

        Returns:
        ---
            List[VariableDefinition]:   The list of VariableDefintions.
        -------------------------------------------------------------------"""
        static_coords = filter(lambda c: hasattr(c, "data"), self.coords.values())
        static_variables = filter(lambda v: hasattr(v, "data"), self.vars.values())
        return list(static_coords) + list(static_variables)
