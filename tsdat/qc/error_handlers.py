import xarray as xr
from abc import abstractmethod
from typing import List, Dict, Any
from tsdat.utils import DSUtil


class QCErrorHandler:
    def __init__(self, tsds: xr.Dataset, params: Dict):
        self.tsds = tsds
        self.params = params

    @abstractmethod
    def run(self, variable_name: str, coordinates: List[int]):
        """
        Perform a follow-on action if a qc test fails

        :param variable_name: Name of the variable that failed
        :param coordinates: n-dimensional data index of the value that failed (i.e., [1246, 1] for [time, height]
        """
        pass


class ReplaceMissing(QCErrorHandler):

    def run(self, variable_name: str, coordinates: List[int]):
        # Set the value at the given coordinates to fill value
        _FillValue = DSUtil.get_fill_value(ds, variable_name)
        var = self.tsds[variable_name]

        if len(coordinates) == 1:
            x = coordinates[0]
            var.values[x] = _FillValue

        elif len(coordinates) == 2:
            x = coordinates[0]
            y = coordinates[1]
            var.values[x][y] = _FillValue

        elif len(coordinates) == 3:
            x = coordinates[0]
            y = coordinates[1]
            z = coordinates[2]
            var.values[x][y][z] = _FillValue


# TODO: possible other error handlers
# tsdat.qc.error_handlers.Fail  # fail the pipeline
# TODO: what about an email handler?
