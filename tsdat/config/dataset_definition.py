import act
import datetime
import numpy as np
import xarray as xr
from typing import Dict, List, Tuple
from .keys import Keys
from .attribute_defintion import AttributeDefinition
from .dimension_definition import DimensionDefinition
from .variable_definition import VariableDefinition


class DatasetDefinition:
    def __init__(self, dictionary: Dict):
        self.attrs = self._parse_attributes(dictionary)
        self.attrs = self._add_required_attributes_and_handles(self.attrs, dictionary.get(Keys.ATTRIBUTES))
        self.dims = self._parse_dimensions(dictionary)
        self.vars = self._parse_variables(dictionary, self.dims)
        self.coords, self.vars = self._parse_coordinates(self.vars)

    def _parse_attributes(self, dictionary: Dict) -> Dict[str, AttributeDefinition]:
        attributes: Dict[str, AttributeDefinition] = {}
        for attr_name, attr_value in dictionary.get(Keys.ATTRIBUTES, {}).items():
            attributes[attr_name] = AttributeDefinition(attr_name, attr_value)
        return attributes
    
    def _add_required_attributes_and_handles(self, attrs: Dict[str, AttributeDefinition], dictionary: Dict) -> Dict[str, AttributeDefinition]:
        """-------------------------------------------------------------------
        Creates handles for several required attributes that must be set 
        before runtime.

        Args:
            dictionary (Dict): The dictionary containing global attributes.
        -------------------------------------------------------------------"""
        # Create handles for required attributes. Throw an Error if attribute
        # is not present
        self.title: str             = dictionary.get("title")
        self.description: str       = dictionary.get("description")
        self.conventions: str       = dictionary.get("conventions")
        self.code_url: str          = dictionary.get("code_url") # TODO: Should this be optional?
        self.location_id: str       = dictionary.get("location_id")
        self.instrument_id: str     = dictionary.get("instrument_id")
        self.data_level: str        = dictionary.get("data_level")

        # Generate attributes that can be generated now -- history and datastream
        # Create handles for each and add to attrs dictionary
        attrs["history"]    = self._generate_history(dictionary)
        attrs["datastream"] = self._generate_datastream(dictionary)
        self.history        = attrs["history"].value
        self.datastream     = attrs["datastream"].value

        return attrs

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

    def _generate_datastream(self, dictionary: Dict) -> AttributeDefinition:
        loc_id      = dictionary.get("location_id")
        instr_id    = dictionary.get("instrument_id")
        qualifier   = dictionary.get("qualifier", "")
        temporal    = dictionary.get("temporal", "")
        data_level  = dictionary.get("data_level")
        datastream_name = f"{loc_id}.{instr_id}{qualifier}{temporal}.{data_level}"
        return AttributeDefinition("datastream", datastream_name)

    def _generate_history(self, dictionary: Dict) -> AttributeDefinition:
        # Should generate a string like: "Ran by user <USER> on machine <MACHINE> at <DATE>"
        # TODO: Add user
        # TODO: Add machine, if possible
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return AttributeDefinition("history", f"Ran at {date}")
    
    def add_input_files_attr(self, input_files: List[str]):
        if input_files is None:
            return
        _input_files = ", ".join(input_files)
        self.attrs["input_files"] = AttributeDefinition("input_files", _input_files)
    
    def get_variable_names(self) -> List[str]:
        return list(self.vars.keys())

    def get_variable(self, variable_name: str) -> VariableDefinition:
        var = self.vars.get(variable_name, None)
        if not var:
            var = self.coords.get(variable_name, None)
        return var
    
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
                # TODO: Warning instead of exception, skip initialization
                raise Exception("Error: coordinate variable {variable.name} must not be empty")
            shape = self.get_variable_shape(variable)
            _FillValue = variable.get_FillValue()
            dtype = variable.get_data_type()
            variable.data = np.full(shape, _FillValue, dtype=dtype)
        
        # If variable has input and is in the dataset, then convert units and 
        # add it to the VariableDefinition
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
