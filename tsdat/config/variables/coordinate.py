from pydantic import model_validator
from typing_extensions import Self

from .variable import Variable


class Coordinate(Variable):
    @model_validator(mode="after")
    def coord_dimensioned_by_self(self) -> Self:
        name, dims = self.name, self.dims
        if [name] != dims:
            raise ValueError(f"coord '{name}' must have dims ['{name}']. Found: {dims}")
        return self
