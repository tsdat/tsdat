# TODO: Implement ZarrWriter

import copy
import xarray as xr
from typing import Any, Dict, Iterable, List, Optional, cast
from pathlib import Path
from pydantic import BaseModel, Extra
from .base import FileWriter

__all__ = ["NetCDFWriter", "CSVWriter", "ParquetWriter"]


class NetCDFWriter(FileWriter):
    """------------------------------------------------------------------------------------
    Thin wrapper around xarray's `Dataset.to_netcdf()` function for saving a dataset to a
    netCDF file. Properties under the `to_netcdf_kwargs` parameter will be passed to
    `Dataset.to_netcdf()` as keyword arguments.

    File compression is used by default to save disk space. To disable compression set the
    `use_compression` parameter to `False`.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        compression_level: int = 1
        """The level of compression to use (0-9). Set to 0 to not use compression."""

        compression_engine: str = "zlib"
        """The compression engine to use."""

        to_netcdf_kwargs: Dict[str, Any] = {}
        """Keyword arguments passed directly to xr.Dataset.to_netcdf()."""

    parameters: Parameters = Parameters()
    file_extension: str = "nc"

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        to_netcdf_kwargs = copy.deepcopy(self.parameters.to_netcdf_kwargs)
        encoding_dict: Dict[str, Dict[str, Any]] = {}
        to_netcdf_kwargs["encoding"] = encoding_dict

        for variable_name in cast(Iterable[str], dataset.variables):

            # Prevent Xarray from setting 'nan' as the default _FillValue
            encoding_dict[variable_name] = dataset[variable_name].encoding  # type: ignore
            if (
                "_FillValue" not in encoding_dict[variable_name]
                and "_FillValue" not in dataset[variable_name].attrs
            ):
                encoding_dict[variable_name]["_FillValue"] = None

            if self.parameters.compression_level:
                encoding_dict[variable_name].update(
                    {
                        self.parameters.compression_engine: True,
                        "complevel": self.parameters.compression_level,
                    }
                )

        dataset.to_netcdf(filepath, **to_netcdf_kwargs)  # type: ignore


class CSVWriter(FileWriter):
    """---------------------------------------------------------------------------------
    Converts a `xr.Dataset` object to a pandas `DataFrame` and saves the result to a csv
    file using `pd.DataFrame.to_csv()`. Properties under the `to_csv_kwargs` parameter
    are passed to `pd.DataFrame.to_csv()` as keyword arguments.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        dim_order: Optional[List[str]] = None
        to_csv_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()
    file_extension: str = "csv"

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        # QUESTION: Can we reliably write the dataset metadata to a separate file such
        # that it can always be retrieved? If not, should we declare this as a format
        # incapable of "round-tripping" (i.e., ds != read(write(ds)) for csv format)?
        df = dataset.to_dataframe(self.parameters.dim_order)  # type: ignore
        df.to_csv(filepath, **self.parameters.to_csv_kwargs)  # type: ignore


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

    parameters: Parameters = Parameters()
    file_extension: str = "parquet"

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        # QUESTION: Can we reliably write the dataset metadata to a separate file such
        # that it can always be retrieved? If not, should we declare this as a format
        # incapable of "round-tripping" (i.e., ds != read(write(ds)) for csv format)?
        df = dataset.to_dataframe(self.parameters.dim_order)  # type: ignore
        df.to_parquet(filepath, **self.parameters.to_parquet_kwargs)  # type: ignore
