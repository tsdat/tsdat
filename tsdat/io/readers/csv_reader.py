from typing import Any, Dict

import pandas as pd
import xarray as xr
from pydantic import BaseModel, Extra

from ..base import DataReader


class CSVReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses pandas and xarray functions to read a csv file and extract its contents into an
    xarray Dataset object. Two parameters are supported: `read_csv_kwargs` and
    `from_dataframe_kwargs`, whose contents are passed as keyword arguments to
    `pandas.read_csv()` and `xarray.Dataset.from_dataframe()` respectively.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        read_csv_kwargs: Dict[str, Any] = {}
        from_dataframe_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: str) -> xr.Dataset:
        df: pd.DataFrame = pd.read_csv(input_key, **self.parameters.read_csv_kwargs)  # type: ignore
        return xr.Dataset.from_dataframe(df, **self.parameters.from_dataframe_kwargs)
