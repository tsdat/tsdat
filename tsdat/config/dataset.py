import logging
import re
from pydantic import (
    Extra,
    Field,
    root_validator,
    validator,
)
from pydantic.fields import ModelField
from typing import Any, Dict, Union
from .attributes import GlobalAttributes
from .utils import YamlModel
from .variables import Variable, Coordinate

__all__ = ["DatasetConfig"]


logger = logging.getLogger(__name__)


class DatasetConfig(YamlModel, extra=Extra.forbid):
    """---------------------------------------------------------------------------------
    Defines the structure and metadata of the dataset produced by a tsdat pipeline.

    Also provides methods to support yaml parsing and validation, including generation
    of json schema.

    Args:
        attrs (GlobalAttributes): Attributes that pertain to the dataset as a whole.
        coords (Dict[str, Coordinate]): The dataset's coordinate variables.
        data_vars (Dict[str, Variable]): The dataset's data variables.

    ---------------------------------------------------------------------------------"""

    # NOTE: it's not currently possible to define a data model for Coordinates as a dict
    # *and* enforce in the schema that it contains certain variables (e.g., time). This
    # gets close, but not enough: https://stackoverflow.com/a/58641115/15641512, so we
    # opted to implement these as dictionaries for now.

    attrs: GlobalAttributes = Field(
        description="Attributes that pertain to the dataset as a whole (as opposed to"
        " attributes that are specific to individual variables."
    )
    coords: Dict[str, Coordinate] = Field(
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
    data_vars: Dict[str, Variable] = Field(
        description="This section defines the data variables that the output dataset"
        " will contain. Variable data can either be retrieved from an input data"
        " source, defined statically via the 'data' property, or initalized to missing"
        " and set dynamically via user code in a tsdat pipeline.",
    )

    @validator("coords")
    @classmethod
    def time_in_coords(cls, coords: Dict[str, Coordinate]) -> Dict[str, Coordinate]:
        if "time" not in coords:
            raise ValueError("Required coordinate definition 'time' is missing.")
        return coords

    @validator("coords", "data_vars")
    def variable_names_are_legal(
        cls, vars: Dict[str, Variable], field: ModelField
    ) -> Dict[str, Variable]:
        for name in vars.keys():
            pattern = re.compile(r"^[a-zA-Z0-9_\(\)\/\[\]\{\}\.]+$")
            if not pattern.match(name):
                raise ValueError(
                    f"'{name}' is not a valid '{field.name}' name. It must be a value"
                    f" matched by {pattern}."
                )
        return vars

    @validator("coords", "data_vars", pre=True)
    @classmethod
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

    def __getitem__(self, name: str) -> Union[Variable, Coordinate]:
        property: Union[Variable, Coordinate]
        try:
            property = self.data_vars[name]
        except KeyError:
            try:
                property = self.coords[name]
            except KeyError:
                logger.error("Key '%s' is neither a data_var nor a coord.", name)
                raise
        return property

    def __contains__(self, __o: object) -> bool:
        return (__o in self.coords) or (__o in self.data_vars)
