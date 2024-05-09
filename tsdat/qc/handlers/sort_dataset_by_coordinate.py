import numpy as np
import xarray as xr
from numpy.typing import NDArray
from pydantic import BaseModel, Extra

from ..base import QualityHandler
from ...utils import record_corrections_applied


class SortDatasetByCoordinate(QualityHandler):
    """------------------------------------------------------------------------------------
    Sorts the dataset by the failed variable, if there are any failures.

    ------------------------------------------------------------------------------------
    """

    class Parameters(BaseModel, extra=Extra.forbid):
        ascending: bool = True
        """Whether to sort the dataset in ascending order. Defaults to True."""

        correction: str = "Coordinate data was sorted in order to ensure monotonicity."

    parameters: Parameters = Parameters()

    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
        failures: NDArray[np.bool_],
    ) -> xr.Dataset:
        if failures.any():
            dataset = dataset.sortby(variable_name, ascending=self.parameters.ascending)  # type: ignore
            record_corrections_applied(
                dataset, variable_name, self.parameters.correction
            )
        return dataset
