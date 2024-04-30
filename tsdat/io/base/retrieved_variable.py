from typing import (
    List,
    Union,
)

from pydantic import BaseModel, Extra

from .data_converter import DataConverter
from ...const import InputKey


# TODO: This needs a better name
class RetrievedVariable(BaseModel, extra=Extra.forbid):
    """Tracks the name of the input variable and the converters to apply."""

    name: Union[str, List[str]]
    data_converters: List[DataConverter] = []
    source: InputKey = ""
