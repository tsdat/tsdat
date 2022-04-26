from .base import FileHandler
from .readers import NetCDFReader, CSVReader
from .writers import NetCDFWriter, CSVWriter

__all__ = ["NetCDFHandler", "CSVHandler"]


class NetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader = NetCDFReader()
    writer: NetCDFWriter = NetCDFWriter()


class CSVHandler(FileHandler):
    extension: str = "csv"
    reader: CSVReader = CSVReader()
    writer: CSVWriter = CSVWriter()
