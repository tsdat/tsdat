from typing import (
    List,
    Union,
)

from pydantic import BaseModel, Extra, Field, validator

from ...const import InputKey
from .data_converter import DataConverter


# TODO: This needs a better name
class RetrievedVariable(BaseModel, extra=Extra.forbid):
    """Tracks the name of the input variable and the converters to apply."""

    name: Union[str, List[str]]
    data_converters: List[DataConverter] = Field(default_factory=list)
    source: InputKey = ""

    @validator("data_converters", always=True)
    def add_units_converter(
        cls, data_converters: list[DataConverter]
    ) -> list[DataConverter]:
        from ..converters.units_converter import UnitsConverter

        if not any(isinstance(dc, UnitsConverter) for dc in data_converters):
            data_converters.append(UnitsConverter())
        return data_converters
