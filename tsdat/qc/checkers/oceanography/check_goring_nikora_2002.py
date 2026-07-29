from pydantic import BaseModel, ConfigDict
import numpy as np
import xarray as xr
from numpy.typing import NDArray

from ...base import QualityChecker


class CheckGoringNikora2002(QualityChecker):
    """The Goring & Nikora 2002 'despiking' method, with Wahl2003 correction.
    Returns a logical vector that is true where spikes are identified."""

    class Parameters(BaseModel):
        model_config = ConfigDict(extra="forbid")
        n_points: int = 5000
        """The number of points over which to perform the method."""

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool_]:
        from mhkit.dolfyn.adv.clean import GN2002

        return GN2002(dataset[variable_name], npt=self.parameters.n_points)
