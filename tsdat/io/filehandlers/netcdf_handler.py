import numpy as np
import xarray as xr
from tsdat.config import Config

from .file_handlers import AbstractFileHandler


class NetCdfHandler(AbstractFileHandler):

    def write(self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
        # We have to make sure that time variables do not have units set as attrs,
        # and instead have units set on the encoding or else xarray will crash
        # when trying to save:
        # https://github.com/pydata/xarray/issues/3739

        for variable_name in ds.variables:
            variable = ds[variable_name]
            if variable.values.dtype.type == np.datetime64:
                units = variable.attrs['units']
                del(variable.attrs['units'])
                variable.encoding['units'] = units

        ds.to_netcdf(filename, format='NETCDF4')

    def read(self, filename: str, config: Config = None, **kwargs) -> xr.Dataset:
        # We are using xr.load_dataset because it will load the whole file
        # into memory and close the underlying file handle.
        return xr.load_dataset(filename)
