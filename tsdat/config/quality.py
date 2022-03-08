from pydantic import BaseModel, Field, validator
from typing import List
from .utils import ParametrizedClass, YamlModel, find_duplicates


class CheckerConfig(ParametrizedClass):
    pass


class HandlerConfig(ParametrizedClass):
    pass


class ManagerConfig(BaseModel):
    # TODO: Validate that apply_to and exclude use valid variable identifiers. This
    # should be done higher up in the model chain.
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


class QualityConfig(YamlModel):
    managers: List[ManagerConfig] = Field(
        description="Register a list of QualityManager(s) that should be used to detect"
        " and handle data quality issues. Each QualityManager configuration block must"
        " consists of a label, a QualityChecker, at least one QualityHandler, and a"
        " list of variables that the manager should be applied to."
    )

    @validator("managers")
    @classmethod
    def validate_manager_names_are_unique(
        cls, v: List[ManagerConfig]
    ) -> List[ManagerConfig]:
        if duplicates := find_duplicates(v):
            raise ValueError(f"Duplicate quality manager names found: {duplicates}")
        return v
