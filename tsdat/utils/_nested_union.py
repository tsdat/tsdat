from typing import Any, Dict


def _nested_union(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Dict[Any, Any]:
    for k, v in dict1.items():
        if isinstance(v, dict):
            node = dict2.setdefault(k, {})
            _nested_union(v, node)  # type: ignore
        else:
            dict2[k] = v
    return dict2
