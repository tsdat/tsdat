import xarray as xr

from io import BytesIO
from typing import Union
from tsdat.config import Config
from .handlers import DataHandler


class NetCdfHandler(DataHandler):
    """FileHandler to read from and write to netCDF files. Takes a number of
    parameters that are passed in from the storage config file. Parameters
    specified in the config file should follow the following example:

    .. code-block:: yaml

        parameters:
          write:
            to_netcdf:
              # Parameters here will be passed to xr.Dataset.to_netcdf()
          read:
            load_dataset:
              # Parameters here will be passed to xr.load_dataset()

    :param parameters:
        Parameters that were passed to the FileHandler when it was registered
        in the storage config file, defaults to {}.
    :type parameters: Dict, optional
    """

    def write(
        self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs
    ) -> None:
        """Saves the given dataset to a netCDF file.

        :param ds: The dataset to save.
        :type ds: xr.Dataset
        :param filename: The path to where the file should be written to.
        :type filename: str
        :param config: Optional Config object, defaults to None
        :type config: Config, optional
        """
        write_params = self.parameters.get("write", {})
        to_netcdf_kwargs = dict(format="NETCDF4")
        to_netcdf_kwargs.update(write_params.get("to_netcdf", {}))

        ds.to_netcdf(filename, **to_netcdf_kwargs)

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
            a bytes-like object. It is passed directly to `xarray.load_dataset()` as the first
            argument.
            name (str, optional): A label to use for the dataset. The DataHandler does not use
            this parameter.

        Returns:
            xr.Dataset: The dataset.

        ------------------------------------------------------------------------------------"""
        read_params = self.parameters.get("read", {})
        load_dataset_kwargs = read_params.get("load_dataset", {})

        return xr.load_dataset(file, **load_dataset_kwargs)
