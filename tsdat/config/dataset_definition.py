import warnings
from typing import Dict, List, Tuple, Any
from .keys import Keys
from .dimension_definition import DimensionDefinition
from .variable_definition import VariableDefinition
from tsdat.exceptions import DefinitionError


class DatasetDefinition:
    """Wrapper for the dataset_definition portion of the pipeline config
    file.

    :param dictionary:
        The portion of the config file corresponding with the dataset
        definition.
    :type dictionary: Dict
    :param datastream_name:
        The name of the datastream that the config file is for.
    :type datastream_name: str
    """

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
        """Extracts the dimensions from the dataset_definition portion of the
        config file.

        :param dictionary:
            The dataset_definition dictionary from the config file.
        :type dictionary: Dict
        :return:
            Returns a mapping of output dimension names to DimensionDefinition
            objects.
        :rtype: Dict[str, DimensionDefinition]
        """
        dimensions: Dict[str, DimensionDefinition] = {}
        for dim_name, dim_dict in dictionary[Keys.DIMENSIONS].items():
            dimensions[dim_name] = DimensionDefinition(dim_name, dim_dict)
        return dimensions

    def _parse_variables(
        self, dictionary: Dict, available_dimensions: Dict[str, DimensionDefinition]
    ) -> Dict[str, VariableDefinition]:
        """Extracts the variables from the dataset_definition portion of the
        config file.

        :param dictionary:
            The dataset_definition dictionary from the config file.
        :type dictionary: Dict
        :param available_dimensions:
            The DimensionDefinition objects that have already been parsed.
        :type available_dimensions: Dict[str, DimensionDefinition]
        :return:
            Returns a mapping of output variable names to VariableDefinition
            objects.
        :rtype: Dict[str, VariableDefinition]
        """
        defaults: Dict[str, Any] = dictionary.get(Keys.DEFAULTS, {})
        variables: Dict[str, VariableDefinition] = {}
        for var_name, var_dict in dictionary[Keys.VARIABLES].items():
            variables[var_name] = VariableDefinition(
                var_name, var_dict, available_dimensions, defaults=defaults
            )
        return variables

    def _parse_coordinates(
        self, vars: Dict[str, VariableDefinition]
    ) -> Tuple[Dict[str, VariableDefinition], Dict[str, VariableDefinition]]:
        """Separates coordinate variables and data variables.

        Determines which variables are coordinate variables and moves those
        variables from ``self.vars`` to ``self.coords``. Coordinate variables
        are defined as variables that are dimensioned by themselves, i.e.,
        ``var.name == var.dim.name`` is a true statement for coordinate
        variables, but false for data variables.

        :param vars: The dictionary of VariableDefinition objects to check.
        :type vars: Dict[str, VariableDefinition]
        :return: The dictionary of dimensions in the dataset.
        :rtype:
            Tuple[Dict[str, VariableDefinition], Dict[str, VariableDefinition]]
        """
        coords = {name: var for name, var in vars.items() if var.is_coordinate()}
        vars = {name: var for name, var in vars.items() if not var.is_coordinate()}
        return coords, vars

    def _validate_dataset_definition(self):
        """Performs sanity checks on the DatasetDefinition object.

        :raises DefinitionError: If any sanity checks fail.
        """
        # Ensure that there is a time coordinate variable
        if "time" not in self.coords:
            # 'time' can sometimes be a scalar, in which case it doesn't need to be a
            # coordinate variable. This is hard to check for at this point, so we make
            # this a warning instead of a DefinitionError.
            warnings.warn("'time' should be defined as a coordinate variable.")

        # Warn if any dimensions do not have an associated coordinate variable
        dims_without_coords = [dim for dim in self.dims if dim not in self.coords]
        for dim in dims_without_coords:
            warnings.warn(
                f"Dimension {dim} does not have an associated coordinate variable."
            )

        # Ensure that all coordinate variables are dimensioned by themselves
        bad_coordinates: List[str] = []
        for coord in self.coords.values():
            if [coord.name] != list(coord.dims.keys()):
                bad_coordinates.append(coord.name)
        if bad_coordinates:
            msg = "Coordinate variables can only be dimensioned by themselves:\n"
            msg += f"bad coordinates: {bad_coordinates}"
            raise DefinitionError(msg)

    def get_attr(self, attribute_name) -> Any:
        """Retrieves the value of the attribute requested, or None if it does
        not exist.

        :param attribute_name: The name of the attribute to retrieve.
        :type attribute_name: str
        :return: The value of the attribute, or None.
        :rtype: Any
        """
        return self.attrs.get(attribute_name, None)

    def get_variable_names(self) -> List[str]:
        """Retrieves the list of variable names. Note that this excludes
        coordinate variables.

        :return: The list of variable names.
        :rtype: List[str]
        """
        return list(self.vars.keys())

    def get_variable(self, variable_name: str) -> VariableDefinition:
        """Attemps to retrieve the requested variable. First searches the data
        variables, then searches the coordinate variables. Returns ``None`` if
        no data or coordinate variables have been defined with the requested
        variable name.

        :param variable_name: The name of the variable to retrieve.
        :type variable_name: str
        :return:
            Returns the VariableDefinition for the variable, or ``None`` if the
            variable could not be found.
        :rtype: VariableDefinition
        """
        variable = self.vars.get(variable_name, None)
        if variable is None:
            variable = self.coords.get(variable_name, None)
        return variable

    def get_coordinates(self, variable: VariableDefinition) -> List[VariableDefinition]:
        """Returns the coordinate VariableDefinition object(s) that dimension
        the requested VariableDefinition.

        :param variable:
            The VariableDefinition whose coordinate variables should be
            retrieved.
        :type variable: VariableDefinition
        :return:
            A list of VariableDefinition coordinate variables that dimension
            the provided VariableDefinition.
        :rtype: List[VariableDefinition]
        """
        coordinate_names = variable.get_coordinate_names()
        return [self.coords.get(coord_name) for coord_name in coordinate_names]

    def get_static_variables(self) -> List[VariableDefinition]:
        """Retrieves a list of static VariableDefinition objects. A variable is
        defined as static if it has a "data" section in the config file, which
        would mean that the variable's data is defined statically. For example,
        in the config file snippet below, "depth" is a static variable:

        .. code-block:: yaml

            depth:
              data: [4, 8, 12]
              dims: [depth]
              type: int
              attrs:
                long_name: Depth
                units: m

        :return: The list of static VariableDefinition objects.
        :rtype: List[VariableDefinition]
        """
        static_coords = filter(lambda c: hasattr(c, "data"), self.coords.values())
        static_variables = filter(lambda v: hasattr(v, "data"), self.vars.values())
        return list(static_coords) + list(static_variables)
