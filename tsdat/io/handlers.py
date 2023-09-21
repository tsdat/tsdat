from pydantic import Field
from .base import FileHandler
from .readers import NetCDFReader, CSVReader, ParquetReader, ZarrReader
from .writers import (
    NetCDFWriter,
    SplitNetCDFWriter,
    CSVWriter,
    ParquetWriter,
    ZarrWriter,
)

__all__ = [
    "NetCDFHandler",
    "SplitNetCDFHandler",
    "CSVHandler",
    "ParquetHandler",
    "ZarrHandler",
]


class NetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader = Field(default_factory=NetCDFReader)
    writer: NetCDFWriter = Field(default_factory=NetCDFWriter)


class SplitNetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader = Field(default_factory=NetCDFReader)
    writer: SplitNetCDFWriter = Field(default_factory=SplitNetCDFWriter)


class CSVHandler(FileHandler):
    extension: str = "csv"
    reader: CSVReader = Field(default_factory=CSVReader)
    writer: CSVWriter = Field(default_factory=CSVWriter)


class ParquetHandler(FileHandler):
    extension: str = "parquet"
    reader: ParquetReader = Field(default_factory=ParquetReader)
    writer: ParquetWriter = Field(default_factory=ParquetWriter)


class ZarrHandler(FileHandler):
    extension: str = "zarr"
    reader: ZarrReader = Field(default_factory=ZarrReader)
    writer: ZarrWriter = Field(default_factory=ZarrWriter)
