"""This module contains the File Handlers that come packaged with tsdat in
addition to methods for registering new File Handler objects."""
from .file_handlers import AbstractFileHandler
from .file_handlers import FileHandler

# These imports register default file handlers
from .csv_handler import CsvHandler
from .netcdf_handler import NetCdfHandler
