# TODO: Implement NetCDFWriter
# TODO: Implement CSVWriter
# TODO: Implement ZarrWriter
# TODO: Implement ParquetWriter

from typing import Any, Dict
import xarray as xr
from pathlib import Path
from pydantic import BaseModel, Extra
from .base import FileWriter


class NetCDFWriterParameters(BaseModel, extra=Extra.forbid):
    use_compression: bool = True
    compression_kwargs: Dict[str, Any] = {"zlib": True, "complevel": 1}
    to_netcdf_kwargs: Dict[str, Any] = {}


class NetCDFWriter(FileWriter):
    parameters: NetCDFWriterParameters = NetCDFWriterParameters()

    def write(self, dataset: xr.Dataset, filepath: Path) -> None:
        if self.parameters.use_compression:
            compression_dict: Dict[Any, Any] = {
                variable_name: self.parameters.compression_kwargs
                for variable_name in dataset.variables
            }
            encoding = self.parameters.to_netcdf_kwargs.get("encoding", {})
            encoding.update(compression_dict)
            self.parameters.to_netcdf_kwargs["encoding"] = encoding

        dataset.to_netcdf(filepath, **self.parameters.to_netcdf_kwargs)  # type: ignore
