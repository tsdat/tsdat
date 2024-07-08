from pydantic import Field

from ..base import FileHandler
from ..readers import A2eCSVReader
from ..writers import (
    A2eCSVWriter,
)


class A2eCSVHandler(FileHandler):
    extension: str = "csv"
    reader: A2eCSVReader = Field(default_factory=A2eCSVReader)
    writer: A2eCSVWriter = Field(default_factory=A2eCSVWriter)
