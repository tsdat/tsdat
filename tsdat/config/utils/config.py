from typing import (
    TypeVar,
)
from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    BaseModel,
)

Config = TypeVar("Config", bound=BaseModel)
