from .base import FileHandler
from .readers import NetCDFReader, CSVReader, ParquetReader
from .writers import NetCDFWriter, CSVWriter, ParquetWriter

__all__ = ["NetCDFHandler", "CSVHandler", "ParquetHandler"]


class NetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader = NetCDFReader()
    writer: NetCDFWriter = NetCDFWriter()


class CSVHandler(FileHandler):
    extension: str = "csv"
    reader: CSVReader = CSVReader()
    writer: CSVWriter = CSVWriter()


class ParquetHandler(FileHandler):
    extension: str = "parquet"
    reader: ParquetReader = ParquetReader()
    writer: ParquetWriter = ParquetWriter()
