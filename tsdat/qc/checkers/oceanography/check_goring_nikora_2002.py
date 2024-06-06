from pydantic import BaseModel, Extra
from numpy.typing import NDArray
import xarray as xr
from mhkit.dolfyn.adv.clean import GN2002

from ...base import QualityChecker


class CheckGoringNikora2002(QualityChecker):
    """----------------------------------------------------------------------------
    The Goring & Nikora 2002 'despiking' method, with Wahl2003 correction.
    Returns a logical vector that is true where spikes are identified.

    Args:
        variable_name (str): array (1D or 3D) to clean.
        n_points (int) : The number of points over which to perform the method.

    Returns:
        mask [np.ndarray]: Logical vector with spikes labeled as 'True'

    ----------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        n_points: int = 5000

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[bool]:
        return GN2002(dataset[variable_name], npt=self.parameters.n_points)
