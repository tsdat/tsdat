import copy
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, cast

import numpy as np
import xarray as xr
from pydantic import Field

from .netcdf_writer import NetCDFWriter
from ...utils import get_filename


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

    ------------------------------------------------------------------------------------
    """

    class Parameters(NetCDFWriter.Parameters):
        time_interval: int = 1
        """Time interval value."""

        time_unit: str = "D"
        """Time interval unit."""

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
            encoding_dict[variable_name] = dataset[variable_name].encoding  # type: ignore
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
