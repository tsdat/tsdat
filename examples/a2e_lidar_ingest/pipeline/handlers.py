import lzma
import xarray as xr
import pandas as pd
from tsdat.io import DataHandler
from tsdat import Config


class StaReader(DataHandler):
    """-------------------------------------------------------------------
    Custom file handler for reading custom *.sta.7z files. These files are
    encoded using cp1252 and the lzma algorithm.

    See https://tsdat.readthedocs.io/ for more `FileHandler` examples.
    -------------------------------------------------------------------"""

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """-------------------------------------------------------------------
        Classes derived from the FileHandler class can implement this method.
        to read a custom file format into a xr.Dataset object.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: An xr.Dataset object
        -------------------------------------------------------------------"""
        lzma_file = lzma.open(
            filename, "rt", encoding="cp1252"
        )  # Default encoding for Windows devices
        df = pd.read_csv(lzma_file, sep="\t", header=41, index_col=False)
        return df.to_xarray()
