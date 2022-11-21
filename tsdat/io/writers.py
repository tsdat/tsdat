import copy
import warnings
import numpy as np
import pandas as pd
import xarray as xr
from typing import Any, Dict, Iterable, List, Optional, cast, Hashable
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
            new_filepath = filepath.with_name(new_filename)  # type: ignore

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
        # QUESTION: Is this format capable of "round-tripping"?
        # (i.e., ds != read(write(ds)) for csv format)
        d1: List[Hashable] = []
        d2: List[Hashable] = []
        for var in dataset:
            shp = dataset[var].shape
            if len(shp) <= 1:
                d1.append(var)
            elif len(shp) == 2:
                d2.append(var)
            else:
                warnings.warn(
                    "CSV writer cannot save variables with more than 2 dimensions."
                )

        name = filepath.stem  # type: ignore

        # Save header data
        header_filepath = filepath.with_suffix(".hdr.csv")  # type: ignore
        header = dataset.attrs
        with open(str(header_filepath), "w", newline="\n") as fp:
            for key in header:
                fp.write(f"{key},{header[key]}\n")

        # Save variable metadata
        metadata_filepath = filepath.with_suffix(".attrs.csv")  # type: ignore
        var_metadata: List[Dict[str, Any]] = []
        for var in dataset:
            attrs = dataset[var].attrs
            attrs.update({"name": var})
            var_metadata.append(attrs)
        df_metadata = pd.DataFrame(var_metadata)
        df_metadata = df_metadata.set_index("name")  # type: ignore
        df_metadata.to_csv(metadata_filepath)

        if d1:
            # Save 1D variables
            d2.extend(
                [v for v in dataset.coords if v != "time"]
            )  # add 2D coordinates to remove list
            ds_1d = dataset.drop_vars(d2)  # drop 2D variables
            df_1d = ds_1d.to_dataframe()
            df_1d.to_csv(filepath, **self.parameters.to_csv_kwargs)  # type: ignore

        if d2:
            # Save 2D variables
            dim2_filepath = filepath.with_suffix(".2D.csv")  # type: ignore
            ds_2d = dataset.drop_vars(d1)  # drop 1D variables
            df_2d = ds_2d.to_dataframe(self.parameters.dim_order)  # type: ignore
            df_2d.to_csv(dim2_filepath, **self.parameters.to_csv_kwargs)  # type: ignore


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
