"""This module contains the File Handlers that come packaged with tsdat in
addition to methods for registering new File Handler objects."""
from .handlers import DataHandler
from .handlers import HandlerRegistry

from .csv import CsvHandler
from .netcdf import NetCdfHandler
from .archive import TarHandler, ZipHandler
