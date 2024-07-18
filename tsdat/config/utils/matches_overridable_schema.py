from typing import (
    Any,
    Dict,
)


def matches_overridable_schema(model_dict: Dict[str, Any]):
    return "path" in model_dict
