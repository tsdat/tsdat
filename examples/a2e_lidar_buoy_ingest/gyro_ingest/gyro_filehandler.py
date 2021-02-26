import pandas as pd
import xarray as xr

from tsdat import Config
from tsdat.io import AbstractFileHandler, register_filehandler

@register_filehandler('.gyro')
class GyroFileHandler(AbstractFileHandler):

    def write(ds: xr.Dataset, filename: str, config: Config, **kwargs):
        pass
    
    # Does not work. Pandas parses each row as a string and does not recognize the 
    # data itself. Sample row from df:
    # WC timestamp\tTime since reset (ms)\tPitch (rad)\tRoll (rad)\tYaw (rad)\tGPS True Heading
    # def read(filename: str, **kwargs) -> xr.Dataset:
    #     df = pd.read_csv(filename)
    #     ds = df.to_xarray()
    #     return xr.Dataset(ds)

    def read(filename: str, **kwargs) -> xr.Dataset:
        df = pd.read_csv(filename, sep="\t", header=0, index_col=False)
        return df.to_xarray() 
