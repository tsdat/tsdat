from typing import (
    Dict,
    NamedTuple,
)

from .retrieved_variable import RetrievedVariable
from .var_name import VarName


class RetrievalRuleSelections(NamedTuple):
    """Maps variable names to the rules and conversions that should be applied."""

    coords: Dict[VarName, RetrievedVariable]
    data_vars: Dict[VarName, RetrievedVariable]
