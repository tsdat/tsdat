from io import BytesIO
import xarray as xr
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional, Union, Pattern
from pydantic import BaseModel, Extra


class BaseDataHandler(BaseModel):
    name: str
    regex: Pattern[str]
    parameters: Dict[str, Any] = {}


class DataReader(BaseDataHandler, ABC):
    @abstractmethod
    def read(
        self, key: Union[str, BytesIO], name: Optional[str] = None
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...


class DataWriter(BaseDataHandler, ABC):
    @abstractmethod
    def write(self, ds: xr.Dataset, key: Optional[str] = None):
        ...


class HandlerRegistry(BaseModel, extra=Extra.forbid):
    # IDEA: Adapt to allow users to plug-in custom Handler registries (e.g., to allow
    # users to customize how reading and writing tasks should be done)

    readers: List[DataReader]
    writers: List[DataWriter]

    # TODO: Consider how DataWriters should interact with the Storage API, given that
    # the HandlerRegistry is a class / property kept within a Storage instance.

    def read(self, key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        """------------------------------------------------------------------------------------
        Dispatches a DataReader given a key (e.g., a filepath, url, or other string). The
        DataReader used will be the first registered DataReader whose regex pattern matches the
        given key. Either an xarray.Dataset or a mapping of some string to xarray.Dataset can
        be returned.

        Generally speaking, most DataReaders (e.g., netCDF, csv, parquet) will simply return an
        xarray.Dataset object pertaining to the input string they are given (e.g., a filepath),
        but DataReaders may also choose to return a dictionary which maps string values to
        xarray.Dataset objects (e.g., zip, tar). It is up to individual DataReader objects to
        determine the type of their output.

        Args:
            key (str): A key which is used both to determine the DataReader to read with, and
            as input to the DataReader.

        Raises:
            ValueError: If no registered DataReaders match the input key a ValueError will be
            raised.

        Returns:
            Union[xr.Dataset, Dict[str, xr.Dataset]]: The data read from the key, as either an
            xarray.Dataset object, or as a Dictionary of key [str] to xarray.Dataset. It is up
            to individual DataReader objects to determine the shape of this output.

        ------------------------------------------------------------------------------------"""
        reader: Optional[DataReader] = None
        for reader in self.readers:
            if reader.regex.match(key):
                break
        if reader is None:
            raise ValueError(f"No input_handler match for key: '{key}'")
        return reader.read(key=key)

    def read_all(self, keys: List[str]) -> Dict[str, xr.Dataset]:
        """------------------------------------------------------------------------------------
        Uses the list of registered DataReaders to read all of the input keys provided. Reading
        is done one key at a time in the order of the keys passed in the list.

        The keys in the returned dictionary are not guaranteed to be the same list of keys as
        those provided as arguments to this method. This is because some DataReader objects may
        return dictionaries themselves (e.g. the built-in ZipReader and TarReader objects
        return mappings like f"{path_to_archive}::{path_within_archive}": xr.Dataset), and
        these entries must be merged into the output mapping.

        Args:
            keys (List[str]): A list of keys which are used both to determine the DataReaders
            to read with, and as inputs to the DataReader.

        Raises:
            ValueError: If any key in the list is not matched by a registered DataReader then a
            ValueError will be raised.

        Returns:
            Dict[str, xr.Dataset]: A dictionary which maps keys to xarray.Dataset objects.

        ------------------------------------------------------------------------------------"""
        # IDEA: Add parallel reading as an optional extra
        output: Dict[str, xr.Dataset] = {}
        for key in keys:
            data = self.read(key=key)
            if isinstance(data, xr.Dataset):
                data = {key: data}
            output.update(data)
        return output

    def write(self, dataset: xr.Dataset):
        """------------------------------------------------------------------------------------
        Dispatches all registered DataWriter instances on the given xarray.Dataset object.

        Args:
            dataset (xr.Dataset): The dataset to write.

        ------------------------------------------------------------------------------------"""
        for writer in self.writers:
            writer.write(dataset)
