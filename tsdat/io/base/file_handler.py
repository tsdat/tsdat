from typing import Any, Dict
from pydantic import field_validator

from .data_handler import DataHandler


class FileHandler(DataHandler):
    """DataHandler specifically tailored to reading and writing files of a specific type."""

    extension: str
    """The specific file extension used for data files, e.g., ".nc"."""

    @field_validator("extension", mode="before")  # type: ignore
    @classmethod
    def no_leading_dot(cls, v: str, values: Dict[str, Any]) -> str:
        return v.lstrip(".")
