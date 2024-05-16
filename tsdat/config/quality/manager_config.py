from pydantic import BaseModel, Extra, Field
from typing import List

from .checker_config import CheckerConfig
from .handler_config import HandlerConfig


class ManagerConfig(BaseModel, extra=Extra.forbid):
    name: str = Field(
        description="A human-readable label that is used to identify this quality"
        " manager."
    )
    checker: CheckerConfig = Field(
        description="Register a class to be used to detect and flag quality issues for"
        " the quality handler(s) registered below to handle.",
    )
    handlers: List[HandlerConfig] = Field(
        min_items=1,
        description="Register one or more handlers to take some action given the"
        " results of the registered checker. Each handler in this list is defined by a"
        " classname (e.g., the python import path to a QualityHandler class), and"
        " (optionally) by a parameters dictionary.",
    )
    apply_to: List[str] = Field(
        min_items=1,
        description="The variables this quality manager should be applied to. Can be"
        ' "COORDS", "DATA_VARS", or any number of individual variable names.',
    )
    exclude: List[str] = []
