from typing import (
    Any,
    Dict,
    Pattern,
)

from ..base import (
    RetrievedVariable,
)
from ...const import InputKey, VarName


class InputKeyRetrievalRules:
    """Gathers variable retrieval rules for the given input key."""

    def __init__(
        self,
        input_key: InputKey,
        coord_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
        data_var_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
    ):
        self.input_key = input_key
        self.coords: Dict[VarName, RetrievedVariable] = {}
        self.data_vars: Dict[VarName, RetrievedVariable] = {}

        for name, retriever_dict in coord_rules.items():
            for pattern, variable_retriever in retriever_dict.items():
                if pattern.match(input_key):
                    self.coords[name] = variable_retriever
                break

        for name, retriever_dict in data_var_rules.items():
            for pattern, variable_retriever in retriever_dict.items():
                if pattern.match(input_key):
                    self.data_vars[name] = variable_retriever
                break
