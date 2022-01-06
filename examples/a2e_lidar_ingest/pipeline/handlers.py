import lzma
import xarray as xr
import pandas as pd

from io import BytesIO
from typing import Union
from tsdat.io import DataHandler


class StaReader(DataHandler):
    """-------------------------------------------------------------------
    Custom file handler for reading custom *.sta.7z files. These files are
    encoded using cp1252 and the lzma algorithm.

    See https://tsdat.readthedocs.io/ for more `DataHandler` examples.
    -------------------------------------------------------------------"""

    def read(
        self,
        file: Union[str, BytesIO],
        name: str = None,
        **kwargs,
    ) -> xr.Dataset:
        assert isinstance(file, str), "Arg 'file' must be a str for this DataHandler."

        # The lidar files use the default encoding for Windows devices
        lzma_file = lzma.open(file, "rt", encoding="cp1252")
        df = pd.read_csv(lzma_file, sep="\t", header=41, index_col=False)
        return df.to_xarray()
