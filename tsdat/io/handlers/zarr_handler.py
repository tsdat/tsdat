from pydantic import Field
from ..base import FileHandler
from ..readers import ZarrReader
from ..writers import (
    ZarrWriter,
)


class ZarrHandler(FileHandler):
    extension: str = "zarr"
    reader: ZarrReader = Field(default_factory=ZarrReader)
    writer: ZarrWriter = Field(default_factory=ZarrWriter)
