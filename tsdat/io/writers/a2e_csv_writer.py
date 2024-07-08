import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import xarray as xr

from ...utils.get_dataset_dim_groups import get_dataset_dim_groups
from ..base import FileWriter

logger = logging.getLogger(__name__)


class A2eCSVWriter(FileWriter):
    file_extension = "csv"

    @staticmethod
    def get_filepath(filepath: Path, dims: tuple[str, ...]) -> Path:
        """Returns the new filepath given the dimensions for the file. E.g., adds the
        ".time.1d.a2e" part of the the filename like so:

        - buoy.z07.a0.20221117.001000.metocean.time.1d.a2e.csv
        - buoy.z07.a0.20221117.001000.metocean.depth.1d.a2e.csv
        - buoy.z07.a0.20221117.001000.metocean.time.depth.2d.a2e.csv
        """
        dims_str = ".".join(dims)
        new_suffix = f".{dims_str}.{len(dims)}d.a2e.csv"

        filepath_str = filepath.as_posix()

        if filepath_str.endswith(".a2e.csv"):
            filepath_str = filepath_str[: -len(".a2e.csv")] + new_suffix
        elif filepath_str.endswith(".csv"):
            filepath_str = filepath_str[: -len(".csv")] + new_suffix
        else:
            raise ValueError  # TODO

        return Path(filepath_str)

    @staticmethod
    def get_metadata_header_str(dataset: xr.Dataset) -> str:
        global_attr_lines: list[str] = []
        variable_attr_lines: list[str] = []

        def _get_global_att_line(attr_name: str, attr_value: Any) -> str:
            if isinstance(attr_value, str):
                return f'{attr_name}="{attr_value}"'
            return f"{attr_name}={attr_value}"

        def _get_var_att_line(var_name: str, attr_name: str, attr_value: Any) -> str:
            if isinstance(attr_value, str):
                return f'{var_name}:{attr_name}="{attr_value}"'
            return f"{var_name}:{attr_name}={attr_value}"

        for attr_name, attr_value in dataset.attrs.items():
            global_attr_lines.append(_get_global_att_line(attr_name, attr_value))

        for coord_name, coord_data in dataset.coords.items():
            if not pd.api.types.is_string_dtype(coord_data.dtype):
                variable_attr_lines.append(f"{coord_name}:dtype={coord_data.dtype}")
            for attr_name, attr_value in coord_data.attrs.items():
                variable_attr_lines.append(
                    _get_var_att_line(str(coord_name), attr_name, attr_value)
                )

        for var_name, var_data in dataset.data_vars.items():
            if not pd.api.types.is_string_dtype(var_data.dtype):
                variable_attr_lines.append(f"{var_name}:dtype={var_data.dtype}")
            for attr_name, attr_value in var_data.attrs.items():
                variable_attr_lines.append(
                    _get_var_att_line(var_name, attr_name, attr_value)
                )

        header = f"header={len(global_attr_lines) + len(variable_attr_lines) + 1}"
        global_metadata = "\n".join(global_attr_lines)
        variable_metadata = "\n".join(variable_attr_lines)

        return "\n".join(filter(None, [header, global_metadata, variable_metadata]))

    @staticmethod
    def get_data_as_str(dataset: xr.Dataset) -> str:
        data_str = (
            dataset.to_dataframe()
            .reset_index(drop=False)
            .to_csv(date_format="%Y-%m-%d %H:%M:%S.%f", header=True, index=False)
        )
        return data_str

    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        assert filepath is not None

        dim_groups = get_dataset_dim_groups(dataset)
        if tuple() in dim_groups.keys():  # dimensionless --> dimensioned by time
            dim_groups[("time",)].extend(dim_groups.pop(tuple()))

        dataset_dim_groups = {dims: dataset[vars] for dims, vars in dim_groups.items()}

        for dims, ds in dataset_dim_groups.items():
            new_filepath = self.get_filepath(filepath, dims)
            metadata_header = self.get_metadata_header_str(ds)
            data_str = self.get_data_as_str(ds)
            result = metadata_header + "\n" + data_str
            new_filepath.write_text(result)
