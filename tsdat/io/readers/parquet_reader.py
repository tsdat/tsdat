from typing import Any, Dict

import pandas as pd
import xarray as xr
from pydantic import BaseModel, Extra

from ..base import DataReader


class ParquetReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses pandas and xarray functions to read a parquet file and extract its contents
    into an xarray Dataset object. Two parameters are supported: `read_parquet_kwargs`
    and `from_dataframe_kwargs`, whose contents are passed as keyword arguments to
    `pandas.read_parquet()` and `xarray.Dataset.from_dataframe()` respectively.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        read_parquet_kwargs: Dict[str, Any] = {}
        from_dataframe_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: str) -> xr.Dataset:
        df: pd.DataFrame = pd.read_parquet(input_key, **self.parameters.read_parquet_kwargs)  # type: ignore
        return xr.Dataset.from_dataframe(df, **self.parameters.from_dataframe_kwargs)
