import os
import numpy as np
import xarray as xr
from tsdat.config import Config
from tsdat.utils import DSUtil

from .file_handlers import AbstractFileHandler


class NetCdfHandler(AbstractFileHandler):
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

        # Remove _FillValue encoding for variables with object dtypes (e.g., strings)
        for variable in ds.variables.values():
            if variable.dtype == np.dtype("O") and "_FillValue" in variable.encoding:
                del variable.encoding["_FillValue"]

        ds.to_netcdf(filename, **to_netcdf_kwargs)

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """Reads in the given file and converts it into an Xarray dataset for
        use in the pipeline.

        :param filename: The path to the file to read in.
        :type filename: str
        :return: A xr.Dataset object.
        :rtype: xr.Dataset
        """
        read_params = self.parameters.get("read", {})
        load_dataset_kwargs = read_params.get("load_dataset", {})
        return xr.load_dataset(filename, **load_dataset_kwargs)


class SplitNetCdfHandler(NetCdfHandler):
    def read(self, filename: str, **kwargs):
        raise NotImplementedError("This FileHandler should not be used to read files.")

    def write(
        self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs
    ) -> None:
        """Saves the given dataset to netCDF file(s) based on the 'time_interval'
        and 'time_unit' config parameters.

        :param ds: The dataset to save.
        :type ds: xr.Dataset
        :param filename: The path to where the file should be written to.
        :type filename: str
        :param config: Optional Config object, defaults to None
        :type config: Config, optional
        """
        storage = kwargs["storage"]

        write_params = self.parameters.get("write", {})
        to_netcdf_kwargs = dict(format="NETCDF4")
        to_netcdf_kwargs.update(write_params.get("to_netcdf", {}))

        # Remove _FillValue encoding for variables with object dtypes (e.g., strings)
        for variable in ds.variables.values():
            if variable.dtype == np.dtype("O") and "_FillValue" in variable.encoding:
                del variable.encoding["_FillValue"]

        # Option to compress netcdf files
        compression = write_params.get("compression", False)
        if compression:
            enc = dict()
            for ky in ds.variables:
                enc[ky] = dict(zlib=True, complevel=1)
            if "encoding" in to_netcdf_kwargs:
                # Overwrite ('update') values in enc with whatever is in kwargs['encoding']
                to_netcdf_kwargs["encoding"] = enc.update(to_netcdf_kwargs["encoding"])
            else:
                to_netcdf_kwargs["encoding"] = enc

        interval = write_params.get("time_interval", "1")
        unit = write_params.get("time_unit", "D")

        t1 = ds.time[0]
        t2 = t1 + np.timedelta64(interval, unit)

        # HACK: The first file is treated differently because FileHandlers are expected
        # to only write to one output file (the 'filename' provided as an argument).
        ds_temp = ds.sel(time=slice(t1, t2))
        ds_temp.to_netcdf(filename, **to_netcdf_kwargs)
        t1 = t2
        t2 = t1 + np.timedelta64(interval, unit)

        while t1 < ds.time[-1]:
            ds_temp = ds.sel(time=slice(t1, t2))

            temp_filedir = filename.rsplit("/")[:-1]
            new_filename = DSUtil.get_dataset_filename(ds_temp)
            temp_filedir.append(new_filename)
            temp_filepath = "/" + os.path.join(
                *temp_filedir
            )  # Write permission denied without "/"

            ds_temp.to_netcdf(temp_filepath, **to_netcdf_kwargs)
            storage.save_local_path(temp_filepath, new_filename)

            t1 = t2
            t2 = t1 + np.timedelta64(interval, unit)
