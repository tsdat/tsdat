@register_filehandler([".nc", ".cdf"])
class NetCdfHandler(AbstractFileHandler):
    @staticmethod
    def write(ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
        ds.to_netcdf(filename, format='NETCDF4')

    @staticmethod
    def read(filename: str, config: Config = None, **kwargs) -> xr.Dataset:
        # TODO: need to have xr.Dataset close the file automatically if user
        #  uses "with" - add a resource manager api
        # ds_disk = xr.open_dataset(filename)
        # TODO: Use config?
        return xr.open_dataset(filename)