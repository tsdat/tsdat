from .base import FileHandler
from .readers import NetCDFReader, CSVReader
from .writers import NetCDFWriter, CSVWriter

class NetCDFHandler(FileHandler):
    extension: str = "nc"
    reader: NetCDFReader
    writer: NetCDFWriter

class CSVHandler(FileHandler):
    extension: str = "csv"
    reader: CSVReader
    writer: CSVWriter
