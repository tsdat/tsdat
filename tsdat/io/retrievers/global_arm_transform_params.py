from collections import defaultdict
import re
from pydantic import BaseModel, Field, validator
from typing import (
    Any,
    Dict,
    Literal,
    Pattern,
)


class GlobalARMTransformParams(BaseModel):
    # TODO: Make this optional
    alignment: Dict[Pattern, Dict[str, Literal["LEFT", "RIGHT", "CENTER"]]]  # type: ignore
    dim_range: Dict[Pattern, Dict[str, str]] = Field(..., alias="range")  # type: ignore
    width: Dict[Pattern, Dict[str, str]]  # type: ignore

    @validator("alignment", "dim_range", "width", pre=True)
    def default_pattern(cls, d: Dict[Any, Any]) -> Dict[Pattern[str], Dict[str, str]]:
        if not d:
            return {}
        pattern_dict: Dict[Pattern[str], Dict[str, str]] = defaultdict(dict)
        for k, v in d.items():
            if isinstance(v, dict):
                pattern_dict[re.compile(k)] = v
            else:
                pattern_dict[re.compile(r".*")][k] = v
        return pattern_dict

    def select_parameters(self, input_key: str) -> Dict[str, Dict[str, Any]]:
        selected_params: Dict[str, Dict[str, Any]] = {
            "alignment": {},
            "range": {},
            "width": {},
        }
        for pattern, params in self.alignment.items():
            if pattern.match(input_key) is not None:
                selected_params["alignment"] = params.copy()
                break

        for pattern, params in self.dim_range.items():
            if pattern.match(input_key) is not None:
                selected_params["range"] = params.copy()
                break

        for pattern, params in self.width.items():
            if pattern.match(input_key) is not None:
                selected_params["width"] = params.copy()
                break

        return selected_params
