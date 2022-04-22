import re
import tempfile
import contextlib
import xarray as xr
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Pattern, Union
from abc import ABC, abstractmethod
from ..utils import ParametrizedClass
from ..config.dataset import DatasetConfig

# TODO: Docstrings. These are all public classes

__all__ = [
    "DataConverter",
    "DataReader",
    "DataWriter",
    "FileWriter",
    "DataHandler",
    "FileHandler",
    "Retriever",
    "Storage",
]


class DataConverter(ParametrizedClass, ABC):
    @abstractmethod
    def convert(
        self,
        dataset: xr.Dataset,
        dataset_config: DatasetConfig,
        variable_name: str,
        **kwargs: Any,
    ) -> xr.Dataset:
        ...


# TODO: VariableFinder
# TODO: DataTransformer


class DataReader(ParametrizedClass, ABC):
    # HACK: Can't do Pattern[str] yet
    regex: Pattern = re.compile(r".*")  # type: ignore

    @abstractmethod
    def read(self, input_key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...

    def matches(self, key: str) -> bool:
        return bool(self.regex.match(key))  # type: ignore


class DataWriter(ParametrizedClass, ABC):
    @abstractmethod
    def write(self, dataset: xr.Dataset, **kwargs: Any) -> None:
        ...


class FileWriter(DataWriter, ABC):

    file_extension: str

    @abstractmethod
    def write(self, dataset: xr.Dataset, filepath: Optional[Path] = None) -> None:
        ...


class DataHandler(ParametrizedClass):
    parameters: Any
    reader: DataReader
    writer: DataWriter


class FileHandler(DataHandler):
    reader: DataReader
    writer: FileWriter


class Retriever(ParametrizedClass, ABC):
    readers: Dict[str, Any]

    @abstractmethod
    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig, **kwargs: Any
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Retrieves the dataset(s) as a mapping like {input_key: xr.Dataset} using the
        registered DataReaders for the retriever.

        Args:
            input_keys (List[str]): The input keys the registered DataReaders should
            read from.
            dataset_config (DatasetConfig): The specification of the output dataset.

        Returns:
            Dict[str, xr.Dataset]: The raw dataset mapping.

        -----------------------------------------------------------------------------"""
        ...


class Storage(ParametrizedClass, ABC):

    parameters: Any = {}
    handler: DataHandler

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
    def uploadable_dir(self, datastream: str) -> Generator[Path, None, None]:
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dirpath = Path(tmp_dir.name)
        try:
            yield tmp_dirpath
        except BaseException:
            raise
        else:
            for path in tmp_dirpath.glob("**/*"):
                if path.is_file():
                    self.save_ancillary_file(path, datastream)
        finally:
            tmp_dir.cleanup()
