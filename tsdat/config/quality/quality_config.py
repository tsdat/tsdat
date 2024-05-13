from pydantic import Extra, Field, validator
from typing import List
from ..utils import YamlModel, find_duplicates

from .manager_config import ManagerConfig


class QualityConfig(YamlModel, extra=Extra.forbid):
    """---------------------------------------------------------------------------------
    Contains quality configuration parameters for tsdat pipelines.

    This class will ultimately be converted into a tsdat.qc.base.QualityManagement class
    for use in downstream tsdat pipeline code.

    Provides methods to support yaml parsing and validation, including the generation of
    json schema for immediate validation.

    Args:
        managers (List[ManagerConfig]): A list of quality checks and controls that
            should be applied.

    ---------------------------------------------------------------------------------"""

    managers: List[ManagerConfig] = Field(
        description="Register a list of QualityManager(s) that should be used to detect"
        " and handle data quality issues. Each QualityManager configuration block must"
        " consists of a label, a QualityChecker, at least one QualityHandler, and a"
        " list of variables that the manager should be applied to."
    )

    @validator("managers")
    def validate_manager_names_are_unique(
        cls, v: List[ManagerConfig]
    ) -> List[ManagerConfig]:
        if duplicates := find_duplicates(v):
            raise ValueError(f"Duplicate quality manager names found: {duplicates}")
        return v
