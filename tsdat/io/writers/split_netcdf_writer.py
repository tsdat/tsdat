import copy
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, cast
from pydantic import Field, field_validator
import pandas as pd
import xarray as xr

from .netcdf_writer import NetCDFWriter
from ...utils import get_filename


class SplitNetCDFWriter(NetCDFWriter):
    """---------------------------------------------------------------------------------
    Wrapper around xarray's `Dataset.to_netcdf()` function for saving a dataset to a
    netCDF file based on a particular time interval, and is an extension of the
    `NetCDFWriter`.
    Each timestamp is written once, in a left-closed, right-open interval beginning at
    the first timestamp; empty intervals are omitted. The final timestamp is included.
    Files are split (sliced) via a time interval specified in two parts, `time_interval`
    a literal value, and a `time_unit` character (year: "Y", month: "M", day: "D", hour:
    "h", minute: "m", second: "s").

    Properties under the `to_netcdf_kwargs` parameter will be passed to
    `Dataset.to_netcdf()` as keyword arguments. File compression is used by default to
    save disk space. To disable compression set the `compression_level` parameter to 0.
    ---------------------------------------------------------------------------------"""

    class Parameters(NetCDFWriter.Parameters):
        time_interval: int = 1
        """Time interval value."""

        time_unit: str = "D"
        """Time interval unit: Y, M, D, h, m, or s."""

        @field_validator("time_interval")
        @classmethod
        def positive_interval(cls, value: int) -> int:
            if value <= 0:
                raise ValueError("time_interval must be positive")
            return value

        @field_validator("time_unit")
        @classmethod
        def supported_unit(cls, value: str) -> str:
            if value not in {"Y", "M", "D", "h", "m", "s"}:
                raise ValueError("time_unit must be Y, M, D, h, m, or s")
            return value

    parameters: Parameters = Field(default_factory=Parameters)
    file_extension: str = "nc"

    def write(
        self,
        dataset: xr.Dataset,
        filepath: Optional[Path] = None,
        **kwargs: Any,
    ) -> None:
        to_netcdf_kwargs = copy.deepcopy(self.parameters.to_netcdf_kwargs)
        encoding_dict: Dict[str, Dict[str, Any]] = {}
        to_netcdf_kwargs["encoding"] = encoding_dict

        for variable_name in cast(Iterable[str], dataset.variables):
            # Prevent Xarray from setting 'nan' as the default _FillValue
            encoding_dict[variable_name] = dataset[variable_name].encoding.copy()  # type: ignore
            if (
                "_FillValue" not in encoding_dict[variable_name]
                and "_FillValue" not in dataset[variable_name].attrs
            ):
                encoding_dict[variable_name]["_FillValue"] = None

            if self.parameters.compression_level:
                # Handle str dtypes: https://github.com/pydata/xarray/issues/2040
                if dataset[variable_name].dtype.kind == "U":
                    encoding_dict[variable_name]["dtype"] = "S1"

                encoding_dict[variable_name].update(
                    {
                        self.parameters.compression_engine: True,
                        "complevel": self.parameters.compression_level,
                    }
                )

            # Must remove original chunksize to split and save dataset
            for key in ("chunksizes", "contiguous"):
                encoding_dict[variable_name].pop(key, None)

        if dataset.sizes.get("time", 0) == 0:
            raise ValueError("Cannot split a dataset without time values")
        if filepath is None:
            raise ValueError("SplitNetCDFWriter requires a filepath")

        interval = self.parameters.time_interval
        unit = self.parameters.time_unit
        offsets = {
            "Y": "years",
            "M": "months",
            "D": "days",
            "h": "hours",
            "m": "minutes",
            "s": "seconds",
        }
        step = pd.DateOffset(**{offsets[unit]: interval})
        t1 = pd.Timestamp(dataset.time.values.min())
        last = pd.Timestamp(dataset.time.values.max())

        while t1 <= last:
            t2 = t1 + step
            ds_temp = dataset.where(
                (dataset.time >= t1) & (dataset.time < t2), drop=True
            )

            if ds_temp.sizes["time"]:
                new_filepath = filepath.with_name(
                    get_filename(ds_temp, self.file_extension)
                )
                ds_temp.to_netcdf(new_filepath, **to_netcdf_kwargs)  # type: ignore

            t1 = t2
