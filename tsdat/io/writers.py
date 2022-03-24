# TODO: Implement NetCDFWriter
# TODO: Implement SplitNetCDFWriter
# TODO: Implement CSVWriter
# TODO: Implement ZarrWriter
# TODO: Implement ParquetWriter

import xarray as xr
from abc import ABC, abstractmethod
from typing import Optional
from tsdat.utils import ParametrizedClass


class DataWriter(ParametrizedClass, ABC):

    # TODO: Nail down method signature
    @abstractmethod
    def write(self, dataset: xr.Dataset, key: Optional[str] = None):
        ...
