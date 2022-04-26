# TODO: Implement ZarrWriter
# TODO: Implement ParquetWriter
import xarray as xr
from typing import Any, Dict, List, Optional
from pathlib import Path
from pydantic import BaseModel, Extra
from ..utils import decode_cf
from .base import FileWriter

__all__ = ["NetCDFWriter", "CSVWriter"]


class NetCDFWriter(FileWriter):
    """------------------------------------------------------------------------------------
    Thin wrapper around xarray's `Dataset.to_netcdf()` function for saving a dataset to a
    netCDF file. Properties under the `to_netcdf_kwargs` parameter will be passed to
    `Dataset.to_netcdf()` as keyword arguments.

    File compression is used by default to save disk space. To disable compression set the
    `use_compression` parameter to `False`.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        use_compression: bool = True
        compression_kwargs: Dict[str, Any] = {"zlib": True, "complevel": 1}
        to_netcdf_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()
    file_extension: str = "nc"

    def write(self, dataset: xr.Dataset, filepath: Optional[Path] = None) -> None:
        if self.parameters.use_compression:
            compression_dict: Dict[Any, Any] = {
                variable_name: self.parameters.compression_kwargs
                for variable_name in dataset.variables
            }
            encoding = self.parameters.to_netcdf_kwargs.get("encoding", {})
            encoding.update(compression_dict)
            self.parameters.to_netcdf_kwargs["encoding"] = encoding

        dataset.to_netcdf(filepath, **self.parameters.to_netcdf_kwargs)  # type: ignore


class CSVWriter(FileWriter):
    """------------------------------------------------------------------------------------
    Converts a `xr.Dataset` object to a pandas `DataFrame` and saves the result to a csv
    file using `pd.DataFrame.to_csv()`. Properties under the `to_csv_kwargs` parameter are
    passed to `pd.DataFrame.to_csv()` as keyword arguments.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        dim_order: Optional[List[str]] = None
        to_csv_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()
    file_extension: str = "csv"

    def write(self, dataset: xr.Dataset, filepath: Optional[Path] = None) -> None:
        # QUESTION: Can we reliably write the dataset metadata to a separate file such
        # that it can always be retrieved? If not, should we declare this as a format
        # incapable of "round-triping" (i.e., ds != read(write(ds)) for csv format)?
        df = dataset.to_dataframe(self.parameters.dim_order)  # type: ignore
        df.to_csv(filepath, **self.parameters.to_csv_kwargs)  # type: ignore
