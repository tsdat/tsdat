# TODO: Implement NetCDFReader
# TODO: Implement CSVReader
# TODO: Implement ZipReader
# TODO: Implement ZarrReader
# TODO: Implement ParquetReader

import xarray as xr
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Dict, Optional, Union
from tsdat.utils import ParametrizedClass


class DataReader(ParametrizedClass, ABC):

    # TODO: Nail down method signature
    @abstractmethod
    def read(
        self, key: Union[str, BytesIO], name: Optional[str] = None
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...
