from typing import (
    Any,
    Dict,
)

from pydantic import validator

from .data_handler import DataHandler
from .data_reader import DataReader
from .file_writer import FileWriter


class FileHandler(DataHandler):
    """---------------------------------------------------------------------------------
    DataHandler specifically tailored to reading and writing files of a specific type.

    Args:
        extension (str): The specific file extension used for data files, e.g., ".nc".
        reader (DataReader): The DataReader subclass responsible for reading input data.
        writer (FileWriter): The FileWriter subclass responsible for writing output
        data.

    ---------------------------------------------------------------------------------"""

    reader: DataReader
    writer: FileWriter
    extension: str

    @validator("extension", pre=True)
    def no_leading_dot(cls, v: str, values: Dict[str, Any]) -> str:
        return v.lstrip(".")
