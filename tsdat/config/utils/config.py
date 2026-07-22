from jsonpointer import set_pointer  # type: ignore
from typing import TypeVar
from pydantic import BaseModel

Config = TypeVar("Config", bound=BaseModel)
