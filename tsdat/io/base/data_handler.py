from typing import Any, Dict
from pydantic import Field, ValidationInfo, field_validator

from ...utils import ParameterizedClass
from .data_reader import DataReader
from .data_writer import DataWriter


class DataHandler(ParameterizedClass):
    """
    Groups a DataReader subclass and a DataWriter subclass together.

    This provides a unified approach to data I/O. DataHandlers are typically expected
    to be able to round-trip the data, i.e. the following psuedocode is generally true:

        `handler.read(handler.write(dataset))) == dataset`

    """

    parameters: Dict[str, Any] = Field(default_factory=dict)

    reader: DataReader
    """The DataReader subclass responsible for reading input data."""

    writer: DataWriter
    """The FileWriter subclass responsible for writing output data."""

    @field_validator("reader", "writer", mode="before")
    @classmethod
    def patch_parameters(cls, v: DataReader, info: ValidationInfo):
        params = info.data.get("parameters", {}).pop(info.field_name, {})
        for param_name, param_value in params.items():
            if isinstance(v.parameters, dict):
                v.parameters[param_name] = param_value
            else:
                setattr(v.parameters, param_name, param_value)
        return v
