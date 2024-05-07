from typing import Any

from pydantic import (
    root_validator,
)

from .variable import Variable


class Coordinate(Variable):
    @root_validator(skip_on_failure=True)
    def coord_dimensioned_by_self(cls, values: Any) -> Any:
        name, dims = values["name"], values["dims"]
        if [name] != dims:
            raise ValueError(f"coord '{name}' must have dims ['{name}']. Found: {dims}")
        return values
