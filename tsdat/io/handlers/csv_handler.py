from pydantic import Field
from ..base import FileHandler
from ..readers import CSVReader
from ..writers import (
    CSVWriter,
)


class CSVHandler(FileHandler):
    extension: str = "csv"
    reader: CSVReader = Field(default_factory=CSVReader)
    writer: CSVWriter = Field(default_factory=CSVWriter)
