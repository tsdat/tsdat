from typing import (
    Any,
    Dict,
)


def matches_overrideable_schema(model_dict: Dict[str, Any]):
    return "path" in model_dict
