from pydantic import BaseModel, ConfigDict, model_validator
from typing_extensions import Self


class AttributeModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def validate_all_ascii(self) -> Self:
        all_values = self.model_dump()
        for key, value in all_values.items():
            if not isinstance(key, str) or not key.isascii():
                raise ValueError(f"'{key}' contains a non-ascii character.")
            if isinstance(value, str) and not value.isascii():
                raise ValueError(
                    f"attr '{key}' -> '{value}' contains a non-ascii character."
                )
        return self
