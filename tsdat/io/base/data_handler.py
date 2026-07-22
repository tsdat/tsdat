from typing import Any, Dict
from typing_extensions import Self
from pydantic import Field, ValidationInfo, field_validator, model_validator

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

    @model_validator(mode="after")
    def patch_parameters(self) -> Self:
        for dataclass in ["reader", "writer"]:
            v = getattr(self, dataclass)
            params = self.parameters.get(dataclass, {})
            for param_name, param_value in params.items():
                if isinstance(v.parameters, dict):
                    v.parameters[param_name] = param_value
                else:
                    setattr(v.parameters, param_name, param_value)
        return self
