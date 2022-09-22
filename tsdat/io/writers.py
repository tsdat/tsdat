import copy
import numpy as np
import xarray as xr
from typing import Any, Dict, Iterable, List, Optional, cast
from pathlib import Path
from pydantic import BaseModel, Extra
from .base import FileWriter
from ..utils import get_filename


__all__ = [
    "NetCDFWriter",
    "SplitNetCDFWriter",
    "CSVWriter",
    "ParquetWriter",
    "ZarrWriter",
]


class NetCDFWriter(FileWriter):
    """------------------------------------------------------------------------------------
    Thin wrapper around xarray's `Dataset.to_netcdf()` function for saving a dataset to a
    netCDF file. Properties under the `to_netcdf_kwargs` parameter will be passed to
    `Dataset.to_netcdf()` as keyword arguments.

    File compression is used by default to save disk space. To disable compression set the
    `compression_level` parameter to `0`.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        compression_level: int = 1
        """The level of compression to use (0-9). Set to 0 to not use compression."""

        compression_engine: str = "zlib"
        """The compression engine to use."""

        to_netcdf_kwargs: Dict[str, Any] = {}
        """Keyword arguments passed directly to xr.Dataset.to_netcdf()."""

    parameters: Parameters = Parameters()
    file_extension: str = ".nc"

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        to_netcdf_kwargs = copy.deepcopy(self.parameters.to_netcdf_kwargs)
        encoding_dict: Dict[str, Dict[str, Any]] = {}
        to_netcdf_kwargs["encoding"] = encoding_dict

        for variable_name in cast(Iterable[str], dataset.variables):
            # Encoding options: https://unidata.github.io/netcdf4-python/#Dataset.createVariable
            # For some reason contiguous=True and chunksizes=None is incompatible with compression
            if hasattr(dataset[variable_name], "encoding"):
                if "contiguous" in dataset[variable_name].encoding:
                    dataset[variable_name].encoding.pop("contiguous")
                if "chunksizes" in dataset[variable_name].encoding:
                    dataset[variable_name].encoding.pop("chunksizes")

            # Prevent Xarray from setting 'nan' as the default _FillValue
            encoding_dict[variable_name] = dataset[variable_name].encoding.copy()  # type: ignore
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


class SplitNetCDFWriter(NetCDFWriter):
    """------------------------------------------------------------------------------------
    Wrapper around xarray's `Dataset.to_netcdf()` function for saving a dataset to a
    netCDF file based on a particular time interval, and is an extension of the
    `NetCDFWriter`.
    Files are split (sliced) via a time interval specified in two parts, `time_interval`
    a literal value, and a `time_unit` character (year: "Y", month: "M", day: "D", hour:
    "h", minute: "m", second: "s").

    Properties under the `to_netcdf_kwargs` parameter will be passed to
    `Dataset.to_netcdf()` as keyword arguments. File compression is used by default to save
    disk space. To disable compression set the `compression_level` parameter to `0`.

    ------------------------------------------------------------------------------------"""

    class Parameters(NetCDFWriter.Parameters):
        time_interval: int = 1
        """Time interval value."""

        time_unit: str = "D"
        """Time interval unit."""

    parameters: Parameters = Parameters()
    file_extension: str = ".nc"

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

            # Must remove original chunksize to split and save dataset
            if "chunksizes" in encoding_dict[variable_name]:
                del encoding_dict[variable_name]["chunksizes"]

        interval = self.parameters.time_interval
        unit = self.parameters.time_unit

        t1 = dataset.time[0]
        t2 = t1 + np.timedelta64(interval, unit)

        while t1 < dataset.time[-1]:
            ds_temp = dataset.sel(time=slice(t1, t2))

            new_filename = get_filename(ds_temp, self.file_extension)
            new_filepath = filepath.with_name(new_filename)

            ds_temp.to_netcdf(new_filepath, **to_netcdf_kwargs)  # type: ignore

            t1 = t2
            t2 = t1 + np.timedelta64(interval, unit)


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
    file_extension: str = ".csv"

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
    file_extension: str = ".parquet"

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        # QUESTION: Can we reliably write the dataset metadata to a separate file such
        # that it can always be retrieved? If not, should we declare this as a format
        # incapable of "round-tripping" (i.e., ds != read(write(ds)) for csv format)?
        df = dataset.to_dataframe(self.parameters.dim_order)  # type: ignore
        df.to_parquet(filepath, **self.parameters.to_parquet_kwargs)  # type: ignore


class ZarrWriter(FileWriter):
    """---------------------------------------------------------------------------------
    Writes the dataset to a basic zarr archive.

    Advanced features such as specifying the chunk size or writing the zarr archive in
    AWS S3 will be implemented later.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        to_zarr_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()
    file_extension: str = ".zarr"

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        encoding_dict: Dict[str, Dict[str, Any]] = {}
        for variable_name in cast(Iterable[str], dataset.variables):
            # Prevent Xarray from setting 'nan' as the default _FillValue
            encoding_dict[variable_name] = dataset[variable_name].encoding  # type: ignore
            if (
                "_FillValue" not in encoding_dict[variable_name]
                and "_FillValue" not in dataset[variable_name].attrs
            ):
                encoding_dict[variable_name]["_FillValue"] = None

        dataset.to_zarr(filepath, encoding=encoding_dict, **self.parameters.to_zarr_kwargs)  # type: ignore
