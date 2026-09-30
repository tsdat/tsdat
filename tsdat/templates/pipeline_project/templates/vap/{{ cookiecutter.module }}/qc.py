import numpy as np
from pydantic import BaseModel, ConfigDict
import xarray as xr
from numpy.typing import NDArray
from tsdat import QualityChecker, QualityHandler


# DEVELOPER: Implement or remove the CustomQualityChecker. If implementing it, please
# rename it to appropriately reflect the check it is performing.
class CustomQualityChecker(QualityChecker):
    """---------------------------------------------------------------------------------
    Custom QualityChecker that can be used to identify issues with the data quality.

    Built-in implementations of quality checkers can be found in the
    [tsdat.qc.checkers](https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/qc/checkers)
    module.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel):
        model_config = ConfigDict(extra="forbid")
        """If your QualityChecker should take any additional arguments from the
        quality configuration file, then those should be specified here.

        e.g.,:
        custom_parameter: float = 5.0

        """

    parameters: Parameters = Parameters()
    """Extra parameters that can be set via the quality configuration file. If you opt
    to not use any configuration parameters then please remove the code above."""

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool_]:

        # True values in the failures array indicate a quality problem.
        var_data = dataset[variable_name]
        failures: NDArray[np.bool_] = np.zeros_like(var_data, dtype=np.bool_)  # type: ignore
        raise NotImplementedError
        return failures


# DEVELOPER: Implement or remove the CustomQualityHandler. If implementing it, please
# rename it to appropriately reflect the action it is performing.
class CustomQualityHandler(QualityHandler):
    """----------------------------------------------------------------------------
    Custom QualityHandler that can be used to correct, report, or otherwise handle
    data quality issues identified by a QualityChecker.

    Built-in implementations of tsdat QualityHandlers can be found in the
    [tsdat.qc.handlers](https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/qc/handlers)
    module.

    ----------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        """If your QualityChecker should take any additional arguments from the
        quality configuration file, then those should be specified here.

        e.g.,:
        custom_parameter: float = 5.0

        """

    parameters: Parameters = Parameters()
    """Extra parameters that can be set via the quality configuration file. If you opt
    to not use any configuration parameters then please remove the code above."""

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool_]
    ) -> xr.Dataset:
        raise NotImplementedError
        return dataset
