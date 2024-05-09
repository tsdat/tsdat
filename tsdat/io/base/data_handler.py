from typing import (
    Any,
    Dict,
)

from pydantic import Field, validator
from pydantic.fields import ModelField

from .data_reader import DataReader
from .data_writer import DataWriter
from ...utils import ParameterizedClass


class DataHandler(ParameterizedClass):
    """---------------------------------------------------------------------------------
    Groups a DataReader subclass and a DataWriter subclass together.

    This provides a unified approach to data I/O. DataHandlers are typically expected
    to be able to round-trip the data, i.e. the following psuedocode is generally true:

        `handler.read(handler.write(dataset))) == dataset`

    Args:
        reader (DataReader): The DataReader subclass responsible for reading input data.
        writer (FileWriter): The FileWriter subclass responsible for writing output
        data.

    ---------------------------------------------------------------------------------"""

    parameters: Dict[str, Any] = Field(default_factory=dict)
    reader: DataReader
    writer: DataWriter

    @validator("reader", "writer", pre=True, check_fields=False, always=True)
    def patch_parameters(cls, v: DataReader, values: Dict[str, Any], field: ModelField):
        params = values.get("parameters", {}).pop(field.name, {})
        for param_name, param_value in params.items():
            if isinstance(v.parameters, dict):
                v.parameters[param_name] = param_value
            else:
                setattr(v.parameters, param_name, param_value)
        return v
