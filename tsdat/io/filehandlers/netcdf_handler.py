import numpy as np
import xarray as xr
from tsdat.config import Config

from .file_handlers import AbstractFileHandler
from .file_handlers import register_filehandler


@register_filehandler([".nc", ".cdf"])
class NetCdfHandler(AbstractFileHandler):
    @staticmethod
    def write(ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
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

    @staticmethod
    def read(filename: str, config: Config = None, **kwargs) -> xr.Dataset:
        # TODO: need to have xr.Dataset close the file automatically if user
        #  uses "with" - add a resource manager api
        # ds_disk = xr.open_dataset(filename)
        # TODO: Use config?
        return xr.open_dataset(filename)