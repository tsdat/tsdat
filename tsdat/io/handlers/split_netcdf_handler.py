from pydantic import Field
from ..base import FileHandler
from ..readers import NetCDFReader
from ..writers import (
    SplitNetCDFWriter,
)


class SplitNetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader = Field(default_factory=NetCDFReader)
    writer: SplitNetCDFWriter = Field(default_factory=SplitNetCDFWriter)
