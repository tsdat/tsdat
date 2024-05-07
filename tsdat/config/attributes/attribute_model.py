from typing import Any, Dict

from pydantic import (
    BaseModel,
    Extra,
    root_validator,
)


class AttributeModel(BaseModel, extra=Extra.allow):
    # HACK: root is needed for now: https://github.com/samuelcolvin/pydantic/issues/515
    @root_validator(skip_on_failure=True)
    def validate_all_ascii(cls, values: Dict[Any, Any]) -> Dict[str, str]:
        for key, value in values.items():
            if not isinstance(key, str) or not key.isascii():
                raise ValueError(f"'{key}' contains a non-ascii character.")
            if isinstance(value, str) and not value.isascii():
                raise ValueError(
                    f"attr '{key}' -> '{value}' contains a non-ascii character."
                )
        return values
