from pathlib import Path
from typing import Any, Dict, Iterable, Optional, cast

import xarray as xr
from pydantic import BaseModel, Extra, Field

from ..base import FileWriter


class ZarrWriter(FileWriter):
    """---------------------------------------------------------------------------------
    Writes the dataset to a basic zarr archive.

    Advanced features such as specifying the chunk size or writing the zarr archive in
    AWS S3 will be implemented later.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        to_zarr_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Field(default_factory=Parameters)
    file_extension: str = "zarr"

    def write(
        self,
        dataset: xr.Dataset,
        filepath: Optional[Path] = None,
        **kwargs: Any,
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

        dataset.to_zarr(
            filepath,
            encoding=encoding_dict,
            **self.parameters.to_zarr_kwargs,
        )  # type: ignore
