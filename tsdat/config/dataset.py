from pydantic import (
    Extra,
    Field,
    constr,
    root_validator,
    validator,
)
from typing import Any, Dict
from .attributes import GlobalAttributes
from .utils import YamlModel
from .variables import Variable, Coordinate

# TEST: constr actually validates stuff
# TEST: schema validation for variable names based on this property
VarName = constr(regex=r"^[a-zA-Z0-9_\(\)\/\[\]\{\}\.]+$")


class DatasetConfig(YamlModel, extra=Extra.forbid):
    """Defines the core output dataset structure, including coordinate variables, data
    variables, and metadata attributes. Quality check variables are not included in this
    structure."""

    # Note: it's not currently possible to define a data model for Coordinates as a dict
    # *and* enforce in the schema that it contains certain variables (e.g., time). This
    # gets closest, but not enough: https://stackoverflow.com/a/58641115/15641512, so I
    # opted to implement these as dictionaries until there's a better solution.

    # TODO: Describe how coords and data vars should be named
    attrs: GlobalAttributes
    coords: Dict[VarName, Coordinate] = Field(
        description="This section defines the coordinate variables that the rest of the"
        " data are dimensioned by. Coordinate variable data can either be retrieved"
        " from an input data source or defined statically via the 'data' property. Note"
        " that tsdat requires the dataset at least be dimensioned by a 'time' variable."
        " Most datasets will only need the 'time' coordinate variable, but"
        " multidimensional datasets (e.g., ADCP or Lidar data (time, height)) are"
        " well-supported. Note that the 'dims' attribute is still required for"
        " coordinate variables, and that this value should be [<name>], where <name> is"
        " the name of the coord (e.g., 'time').",
    )
    data_vars: Dict[VarName, Variable] = Field(
        description="This section defines the data variables that the output dataset"
        " will contain. Variable data can either be retrieved from an input data"
        " source, defined statically via the 'data' property, or initalized to missing"
        " and set dynamically via user code in a tsdat pipeline.",
    )

    @validator("coords")
    @classmethod
    def time_in_coords(
        cls, coords: Dict[VarName, Coordinate]
    ) -> Dict[VarName, Coordinate]:
        if "time" not in coords:
            raise ValueError("Required coordinate definition 'time' is missing.")
        return coords

    # @validator("coords", "data_vars")
    # def variable_names_are_legal(
    #     cls, vars: Dict[str, Variable], field: ModelField
    # ) -> Dict[str, Variable]:
    #     for name in vars.keys():
    #         pattern = re.compile(r"^[a-zA-Z0-9_\(\)\/\[\]\{\}\.]+$")
    #         if not pattern.match(name):
    #             raise ValueError(
    #                 f"'{name}' is not a valid '{field.name}' name. It must be a value"
    #                 f" matched by {pattern}."
    #             )
    #     return vars

    @validator("coords", "data_vars", pre=True)
    def set_variable_name_property(
        cls, vars: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        for name in vars.keys():
            vars[name]["name"] = name
        return vars

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_variable_name_uniqueness(cls, values: Any) -> Any:
        coord_names = set(values["coords"].keys())
        var_names = set(values["data_vars"].keys())

        if duplicates := coord_names.intersection(var_names):
            raise ValueError(
                "Variables cannot be both coords and data_vars:"
                f" {sorted(list(duplicates))}."
            )
        return values