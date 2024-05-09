import copy
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, cast

import xarray as xr
from pydantic import BaseModel, Extra, Field

from ..base import FileWriter


class NetCDFWriter(FileWriter):
    """------------------------------------------------------------------------------------
    Thin wrapper around xarray's `Dataset.to_netcdf()` function for saving a dataset to a
    netCDF file. Properties under the `to_netcdf_kwargs` parameter will be passed to
    `Dataset.to_netcdf()` as keyword arguments.

    File compression is used by default to save disk space. To disable compression set the
    `compression_level` parameter to `0`.

    ------------------------------------------------------------------------------------
    """

    class Parameters(BaseModel, extra=Extra.forbid):
        compression_level: int = 1
        """The level of compression to use (0-9). Set to 0 to not use compression."""

        compression_engine: str = "zlib"
        """The compression engine to use."""

        to_netcdf_kwargs: Dict[str, Any] = {}
        """Keyword arguments passed directly to xr.Dataset.to_netcdf()."""

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

            # Remove unexpected netCDF4 encoding parameters
            # https://github.com/pydata/xarray/discussions/5709
            params = [
                "szip",
                "zstd",
                "bzip2",
                "blosc",
                "contiguous",
                "chunksizes",
                "preferred_chunks",
            ]
            [
                encoding_dict[variable_name].pop(p)
                for p in params
                if p in encoding_dict[variable_name]
            ]

            if self.parameters.compression_level and (
                dataset[variable_name].dtype.kind not in ["U", "O"]
            ):
                encoding_dict[variable_name].update(
                    {
                        self.parameters.compression_engine: True,
                        "complevel": self.parameters.compression_level,
                    }
                )

        # Handle str dtypes: https://github.com/pydata/xarray/issues/2040
        if dataset[variable_name].dtype.kind == "U":
            encoding_dict[variable_name]["dtype"] = "str"

        if "time" in dataset.dims:
            to_netcdf_kwargs["unlimited_dims"] = set(
                ["time"] + list(dataset.encoding.get("unlimited_dims", []))
            )

        dataset.to_netcdf(filepath, **to_netcdf_kwargs)  # type: ignore
