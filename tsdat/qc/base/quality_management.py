from typing import List

import xarray as xr
from pydantic import BaseModel, Extra

from .quality_manager import QualityManager


class QualityManagement(BaseModel, extra=Extra.forbid):
    """---------------------------------------------------------------------------------
    Main class for orchestrating the dispatch of QualityCheckers and QualityHandlers.

    Args:
        managers (List[QualityManager]): The list of QualityManagers that should be run.

    ---------------------------------------------------------------------------------"""

    managers: List[QualityManager]

    def manage(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Runs the registered QualityManagers on the dataset.

        Args:
            dataset (xr.Dataset): The dataset to apply quality checks and controls to.

        Returns:
            xr.Dataset: The quality-checked dataset.

        -----------------------------------------------------------------------------"""
        for manager in self.managers:
            dataset = manager.run(dataset)
        return dataset
