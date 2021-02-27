import pandas as pd
import xarray as xr

from tsdat import Config
from tsdat.io import AbstractFileHandler, register_filehandler

@register_filehandler('.sta')
class StaFileHandler(AbstractFileHandler):

    def write(ds: xr.Dataset, filename: str, config: Config, **kwargs):
        raise NotImplementedError("Error: this file format should not be used to write to.")

    def read(filename: str, **kwargs) -> xr.Dataset:
        df = pd.read_csv(filename, sep="\t", header=41, index_col=False)
        return df.to_xarray() 
