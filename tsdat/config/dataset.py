from pydantic import (
    Extra,
    Field,
    root_validator,
    validator,
)
from typing import Any, List
from .attributes import GlobalAttributes
from .utils import YamlModel, find_duplicates
from .variables import Variable, Coordinate


class DatasetDefinition(YamlModel, extra=Extra.forbid):
    """Defines the core output dataset structure, including coordinate variables, data
    variables, and metadata attributes. Quality check variables are not included in this
    structure."""

    # Note: it's not currently possible to define a data model for Coordinates as a dict
    # *and* enforce in the yaml that it contains certain variables (e.g., time). This
    # gets closest, but not enough: https://stackoverflow.com/a/58641115/15641512, so I
    # opted to implement these as a List[Coordinate] and List[Variable] until there's a
    # better solution.

    attrs: GlobalAttributes
    coords: List[Coordinate] = Field(
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
    data_vars: List[Variable] = Field(
        description="This section defines the data variables that the output dataset"
        " will contain. Variable data can either be retrieved from an input data"
        " source, defined statically via the 'data' property, or initalized to missing"
        " and set dynamically via user code in a tsdat pipeline.",
    )

    @validator("coords")
    @classmethod
    def time_in_coords(cls, coords: List[Coordinate]) -> List[Coordinate]:
        if "time" not in [coord.name for coord in coords]:
            raise ValueError("Required coord definition 'time' is missing.")
        return coords

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_variable_name_uniqueness(cls, values: Any) -> Any:

        if duplicates := find_duplicates(values["coords"]):
            raise ValueError(f"Duplicate coord names are not allowed: {duplicates}")

        if duplicates := find_duplicates(values["data_vars"]):
            raise ValueError(f"Duplicate data_var names are not allowed: {duplicates}")

        all_vars: List[Variable] = values["coords"] + values["data_vars"]
        if duplicates := find_duplicates(all_vars):
            raise ValueError(
                f"Variables cannot be both coords and data_vars: {tuple(duplicates)}."
            )
        return values
