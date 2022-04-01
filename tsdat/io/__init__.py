"""The tsdat.io package provides the classes that the data pipeline
uses to manage I/O for the pipeline.  Specifically, it includes:

    #. The FileHandler infrastructure used to read/write to/from
       specific file formats, and
    #. The Storage infrastructure used to store/access processed
       data files

 We warmly welcome community contribututions to increase the list of
 supported FileHandlers and Storage destinations.
"""
from .filehandlers import (
    AbstractFileHandler,
    FileHandler,
    register_filehandler,
    CsvHandler,
    NetCdfHandler,
    SplitNetCdfHandler,
)
from .storage import (
    DatastreamStorage,
    TemporaryStorage,
    DisposableLocalTempFile,
    DisposableStorageTempFileList,
    DisposableLocalTempFileList,
)
from .filesystem_storage import FilesystemStorage
from .aws_storage import AwsStorage, S3Path
