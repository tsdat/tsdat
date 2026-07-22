from pydantic import BaseModel, field_validator
from typing import Optional


class GlobalFetchParams(BaseModel):
    time_padding: Optional[str]  # type: ignore
    """How far in time to look ahead (+), behind (-), or both to search
    for files."""

    @field_validator("time_padding", mode="before")
    @classmethod
    def default_to_seconds(cls, d: str) -> str:
        if not d:
            return ""
        elif d[-1].isnumeric():
            return d + "s"
        else:
            return d

    @staticmethod
    def get_direction(d: str) -> tuple[int, str]:
        if "+" in d:
            return 1, d.replace("+", "")
        elif "-" in d:
            return -1, d.replace("-", "")
        else:
            return 0, d
