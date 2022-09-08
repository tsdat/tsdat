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
    extension: str = ".nc"
    reader: NetCDFReader = NetCDFReader()
    writer: NetCDFWriter = NetCDFWriter()


class SplitNetCDFHandler(FileHandler):
    extension: str = ".nc"
    reader: NetCDFReader = NetCDFReader()
    writer: SplitNetCDFWriter = SplitNetCDFWriter()


class CSVHandler(FileHandler):
    extension: str = ".csv"
    reader: CSVReader = CSVReader()
    writer: CSVWriter = CSVWriter()


class ParquetHandler(FileHandler):
    extension: str = ".parquet"
    reader: ParquetReader = ParquetReader()
    writer: ParquetWriter = ParquetWriter()


class ZarrHandler(FileHandler):
    extension: str = ".zarr"
    reader: ZarrReader = ZarrReader()
    writer: ZarrWriter = ZarrWriter()
