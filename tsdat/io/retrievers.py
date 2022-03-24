# TODO: Implement SimpleRetriever
# TODO: Implement MultiDatastreamRetriever

import xarray as xr
from abc import ABC, abstractmethod
from typing import Dict, List
from tsdat.utils import ParametrizedClass


class BaseRetriever(ParametrizedClass, ABC):

    # TODO: Nail down method signature
    @abstractmethod
    def retrieve_raw_datasets(self, input_keys: List[str]) -> Dict[str, xr.Dataset]:
        ...
