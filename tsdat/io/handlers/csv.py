import os
import yaml
import pandas as pd
import xarray as xr

from io import BytesIO

from typing import Union
from tsdat.config import Config
from tsdat.utils import DSUtil
from .handlers import DataHandler


class CsvHandler(DataHandler):
    """FileHandler to read from and write to CSV files. Takes a number of
    parameters that are passed in from the storage config file. Parameters
    specified in the config file should follow the following example:

    .. code-block:: yaml

        parameters:
          write:
            to_dataframe:
              # Parameters here will be passed to xr.Dataset.to_dataframe()
            to_csv:
              # Parameters here will be passed to pd.DataFrame.to_csv()
          read:
            read_csv:
              # Parameters here will be passed to pd.read_csv()
            to_xarray:
              # Parameters here will be passed to pd.DataFrame.to_xarray()

    :param parameters:
        Parameters that were passed to the FileHandler when it was registered
        in the storage config file, defaults to {}.
    :type parameters: Dict, optional
    """

    def write(
        self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs
    ) -> None:
        """Saves the given dataset to a csv file.

        :param ds: The dataset to save.
        :type ds: xr.Dataset
        :param filename: The path to where the file should be written to.
        :type filename: str
        :param config: Optional Config object, defaults to None
        :type config: Config, optional
        """
        if len(ds.dims) > 1:
            raise TypeError(
                "Dataset has more than one dimension, so it can't be saved to csv.  Try netcdf instead."
            )

        write_params = self.parameters.get("write", {})
        to_dataframe_kwargs = write_params.get("to_dataframe", {})
        to_csv_kwargs = write_params.get("to_csv", {})

        df = ds.to_dataframe(**to_dataframe_kwargs)
        df.to_csv(filename, **to_csv_kwargs)

        yaml_filename = f"{filename}.yaml"
        with open(yaml_filename, "w") as file:
            metadata = DSUtil.get_metadata(ds)
            yaml.dump(metadata, file)

    def read(
        self,
        file: Union[str, BytesIO],
        name: str = None,
        **kwargs,
    ) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Reads the given file into a pandas DataFrame before converting it into an xarray
        Dataset for use in the pipeline.

        Args:
            file (Union[str, BytesIO]): The file to read in. Can be provided as a filepath or
            a bytes-like object. It is passed directly to `pandas.read_csv()` as the first
            argument.
            name (str, optional): A label to use for the dataset. The DataHandler does not use
            this parameter.

        Returns:
            xr.Dataset: The dataset.

        ------------------------------------------------------------------------------------"""
        read_params = self.parameters.get("read", {})
        read_csv_kwargs = read_params.get("read_csv", {})
        to_xarray_kwargs = read_params.get("to_xarray", {})

        df = pd.read_csv(file, **read_csv_kwargs)
        ds: xr.Dataset = df.to_xarray(**to_xarray_kwargs)

        if isinstance(file, str):
            yaml_filename = f"{file}.yaml"
            if os.path.isfile(yaml_filename):
                with open(yaml_filename, "r") as metadata_file:
                    metadata = yaml.safe_load(metadata_file)
                    ds.attrs = metadata.get("attributes", {})
                    for variable, attrs in metadata.get("variables", {}).items():
                        ds[variable].attrs = attrs

        return ds
