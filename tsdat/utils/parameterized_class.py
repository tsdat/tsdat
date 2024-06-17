from typing import Any
from pydantic import BaseModel, Extra, Field


class ParameterizedClass(BaseModel, extra=Extra.forbid):
    """------------------------------------------------------------------------------------
    Base class for any class that accepts 'parameters' as an argument.

    Sets the default 'parameters' to {}. Subclasses of ParameterizedClass should override
    the 'parameters' properties to support custom required or optional arguments from
    configuration files.

    ------------------------------------------------------------------------------------
    """

    parameters: Any = Field(default_factory=dict)
