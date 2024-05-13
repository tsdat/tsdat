from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Any,
    Optional,
)

import xarray as xr
from pydantic import validator

from .data_writer import DataWriter


class FileWriter(DataWriter, ABC):
    """---------------------------------------------------------------------------------
    Base class for file-based DataWriters.

    Args:
        file_extension (str): The file extension that the FileHandler should be used
            for, e.g., ".nc", ".csv", ...

    ---------------------------------------------------------------------------------"""

    file_extension: str

    @validator("file_extension")
    def no_leading_dot(cls, v: str) -> str:
        return v.lstrip(".")

    @abstractmethod
    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        """-----------------------------------------------------------------------------
        Writes the dataset to the provided filepath.

        This method is typically called by the tsdat storage API, which will be
        responsible for providing the filepath, including the file extension.

        Args:
            dataset (xr.Dataset): The dataset to save.
            filepath (Optional[Path]): The path to the file to save.

        -----------------------------------------------------------------------------"""
        ...
