from pydantic import Field
from ..base import FileHandler
from ..readers import ParquetReader
from ..writers import (
    ParquetWriter,
)


class ParquetHandler(FileHandler):
    extension: str = "parquet"
    reader: ParquetReader = Field(default_factory=ParquetReader)
    writer: ParquetWriter = Field(default_factory=ParquetWriter)
