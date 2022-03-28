import re
import tempfile
import contextlib
import xarray as xr
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Pattern, Union
from abc import ABC, abstractmethod
from tsdat.utils import ParametrizedClass
from tsdat.config.dataset import DatasetConfig

# TODO: Docstrings. These are all public classes

__all__ = [
    "DataConverter",
    "DataReader",
    "Retriever",
    "FileWriter",
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


# TODO: VariableFinder
# TODO: DataTransformer


class DataReader(ParametrizedClass, ABC):
    regex: Pattern = re.compile(r".*")  # type: ignore # HACK: Can't do Pattern[str] yet

    @abstractmethod
    def read(self, input_key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...

    def matches(self, key: str) -> bool:
        return bool(self.regex.match(key))  # type: ignore


class Retriever(ParametrizedClass, ABC):
    readers: Any

    @abstractmethod
    def retrieve_raw_datasets(self, input_keys: List[str]) -> Dict[str, xr.Dataset]:
        ...

    @abstractmethod
    def merge_raw_datasets(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
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
