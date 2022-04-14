import re
import tempfile
import contextlib
import xarray as xr
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Pattern, Union
from numpy.typing import NDArray
from abc import ABC, abstractmethod
from tsdat.utils import ParametrizedClass
from tsdat.config import DatasetConfig

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
    def convert(
        self,
        dataset: xr.Dataset,
        dataset_config: DatasetConfig,
        variable_name: str,
        **kwargs: Any,
    ) -> xr.Dataset:
        ...

    def assign_data(
        self, dataset: xr.Dataset, data: NDArray[Any], variable_name: str
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Assigns converted data to the specified variable in the dataset.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to reassign.
            data (NDArray[Any]): The converted data to assign.
            variable_name (str): The name of the variable in the dataset.

        Returns:
            xr.Dataset: The dataset with the new data assigned to the specified
            variable.

        -----------------------------------------------------------------------------"""
        if not variable_name in dataset.coords:
            dataset[variable_name].data = data
        else:
            tmp_name = f"__{variable_name}__"
            dataset[tmp_name] = xr.zeros_like(dataset[variable_name], dtype=data.dtype)  # type: ignore
            dataset[tmp_name].data = data
            dataset = dataset.swap_dims({variable_name: tmp_name})  # type: ignore
            dataset = dataset.drop_vars(variable_name)
            dataset = dataset.rename_dims({tmp_name: variable_name})
            dataset = dataset.rename_vars({tmp_name: variable_name})

        return dataset


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
    readers: Any

    @abstractmethod
    def retrieve(self, input_keys: List[str], **kwargs: Any) -> Dict[str, xr.Dataset]:
        ...

    @abstractmethod
    def prepare(
        self,
        raw_mapping: Dict[str, xr.Dataset],
        dataset_config: DatasetConfig,
        **kwargs: Any,
    ) -> xr.Dataset:
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
                if path.is_file:
                    self.save_ancillary_file(path, datastream)
        finally:
            tmp_dir.cleanup()
