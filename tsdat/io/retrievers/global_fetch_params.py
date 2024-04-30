from pydantic import BaseModel, validator
from typing import (
    Optional,
)


class GlobalFetchParams(BaseModel):
    time_padding: Optional[str]  # type: ignore
    """How far in time to look ahead (+), behind (-), or both to search
    for files."""

    # TODO: Seems like a static method here, should refactor into as such.
    @validator("time_padding", pre=True)
    def default_to_seconds(cls, d: str) -> str:
        if not d:
            return ""
        elif d[-1].isnumeric():
            return d + "s"
        else:
            return d

    # TODO: Method definition says that a lone `int` is returned, but return statements return
    #  a `tuple[int, str]`. This should be corrected.
    # TODO: Seems like a static method here, should refactor into as such.
    def get_direction(self, d: str) -> int:
        if "+" in d:
            return 1, d.replace("+", "")
        elif "-" in d:
            return -1, d.replace("-", "")
        else:
            return 0, d
