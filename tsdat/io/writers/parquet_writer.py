from pathlib import Path
from typing import Any, Dict, List, Optional

import xarray as xr
from pydantic import BaseModel, Extra, Field

from ..base import FileWriter


class ParquetWriter(FileWriter):
    """---------------------------------------------------------------------------------
    Writes the dataset to a parquet file.

    Converts a `xr.Dataset` object to a pandas `DataFrame` and saves the result to a
    parquet file using `pd.DataFrame.to_parquet()`. Properties under the
    `to_parquet_kwargs` parameter are passed to `pd.DataFrame.to_parquet()` as keyword
    arguments.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        dim_order: Optional[List[str]] = None
        to_parquet_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Field(default_factory=Parameters)
    file_extension: str = "parquet"

    def write(
        self,
        dataset: xr.Dataset,
        filepath: Optional[Path] = None,
        **kwargs: Any,
    ) -> None:
        # QUESTION: Can we reliably write the dataset metadata to a separate file such
        # that it can always be retrieved? If not, should we declare this as a format
        # incapable of "round-tripping" (i.e., ds != read(write(ds)) for csv format)?
        df = dataset.to_dataframe(self.parameters.dim_order)  # type: ignore
        df.to_parquet(filepath, **self.parameters.to_parquet_kwargs)  # type: ignore
