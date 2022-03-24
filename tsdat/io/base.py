import contextlib
from datetime import datetime
from pathlib import Path
import tempfile
from typing import Any, Dict, Generator, List, Union
import xarray as xr
from abc import ABC, abstractmethod
from tsdat.utils import ParametrizedClass
from tsdat.config.dataset import DatasetConfig

# TODO: Docstrings. These are all public classes

__all__ = [
    "DataConverter",
    "DataReader",
    "Retriever",
    "Storage",
]


class DataConverter(ParametrizedClass, ABC):
    @abstractmethod
    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
        dataset_config: DatasetConfig,
        **kwargs: Any,
    ) -> xr.Dataset:
        ...


# TODO: DataFinder
# TODO: DataTransformer


class DataReader(ParametrizedClass, ABC):
    @abstractmethod
    def read(
        self, input_key: str, dataset_config: DatasetConfig
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...


class Retriever(ParametrizedClass, ABC):
    readers: Any

    @abstractmethod
    def retrieve_raw_datasets(
        self, input_keys: List[str], dataset_config: DatasetConfig
    ) -> Dict[str, xr.Dataset]:
        ...

    @abstractmethod
    def merge_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset], dataset_config: DatasetConfig
    ) -> xr.Dataset:
        ...


class FileWriter(ParametrizedClass, ABC):
    @abstractmethod
    def write(self, dataset: xr.Dataset, filepath: Path) -> None:
        ...


class Storage(ParametrizedClass, ABC):

    parameters: Any = {}
    writers: Any

    @abstractmethod
    def save_data(self, dataset: xr.Dataset):
        ...

    # @abstractmethod
    # def delete_data(self, start: datetime, end: datetime, datastream: str):
    #     ...
    # @abstractmethod
    # def find_data(self, start: datetime, end: datetime, datastream: str):
    #     ...

    @abstractmethod
    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        ...

    @abstractmethod
    def save_ancillary_file(self, filepath: Path, datastream: str):
        ...

    @contextlib.contextmanager
    def uploadable_tmp_dir(self, datastream: str) -> Generator[Path, None, None]:
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dirpath = Path(tmp_dir.name)
        try:
            yield tmp_dirpath
        except BaseException:
            raise
        else:
            for path in tmp_dirpath.glob("**/*"):
                if path.is_file:
                    self.save_ancillary_file(path, datastream)
        finally:
            tmp_dir.cleanup()
