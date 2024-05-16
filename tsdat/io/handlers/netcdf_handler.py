from pydantic import Field
from ..base import FileHandler
from ..readers import NetCDFReader
from ..writers import (
    NetCDFWriter,
)


class NetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader = Field(default_factory=NetCDFReader)
    writer: NetCDFWriter = Field(default_factory=NetCDFWriter)
