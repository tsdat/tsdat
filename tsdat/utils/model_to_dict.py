from typing import Any, Dict
from pydantic import BaseModel

from ._nested_union import _nested_union


# Brilliant solution seen here https://stackoverflow.com/a/65363852/15641512
def model_to_dict(model: BaseModel, by_alias: bool = True) -> Dict[Any, Any]:
    """---------------------------------------------------------------------------------
    Converts the model to a dict with unset optional properties excluded.

    Performs a nested union on the dictionaries produced by setting the `exclude_unset`
    and `exclude_none` options to True for the `model.dict()` method. This allows for
    the preservation of explicit `None` values in the yaml, while still filtering out
    values that default to `None`.

    Borrowed approximately from https://stackoverflow.com/a/65363852/15641512.


    Args:
        model (BaseModel): The pydantic model to dict-ify.

    Returns:
        Dict[Any, Any]: The model as a dictionary.

    ---------------------------------------------------------------------------------"""
    return _nested_union(
        model.dict(exclude_unset=True, by_alias=by_alias),
        model.dict(exclude_none=True, by_alias=by_alias),
    )
