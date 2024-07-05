import re
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import xarray as xr
from pydantic import BaseModel, Extra

from ..base import DataReader


class A2eCSVReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses pandas and xarray functions to read a csv file and extract its contents into an
    xarray Dataset object. Two parameters acre supported: `read_csv_kwargs` and
    `from_dataframe_kwargs`, whose contents are passed as keyword arguments to
    `pandas.read_csv()` and `xarray.Dataset.from_dataframe()` respectively.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        read_csv_kwargs: Dict[str, Any] = {}
        from_dataframe_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    @staticmethod
    def get_dims_from_filename(input_key: str) -> list[str]:
        """Parses the input key / filename for the expected dimensions contained in the
        file. This is possible because A2e CSV filenames follow a standardized format,
        e.g.:

        - buoy.z07.a0.20221117.001000.metocean.time.1d.a2e.csv
        - buoy.z07.a0.20221117.001000.metocean.depth.1d.a2e.csv
        - buoy.z07.a0.20221117.001000.metocean.time.depth.2d.a2e.csv
        """
        parts = input_key.split(".")
        # assert re.match(r"^\dd$", parts[-3]), f"{input_key} does not end in <1/2/3..>d.a2e.csv"
        n_dims = int(parts[-3][:-1])
        dims = parts[-3 - n_dims : -3]
        return dims

    @staticmethod
    def parse_attributes(text: str) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
        global_attributes: dict[str, Any] = {}
        variable_attributes: dict[str, dict[str, Any]] = {}

        metadata_pattern = re.compile(r"^(\w+)=(.+)$", re.MULTILINE)
        variable_pattern = re.compile(r"^(\w+):(\w+)=(.+)$", re.MULTILINE)

        for att_name, att_value in re.findall(metadata_pattern, text):
            global_attributes[att_name] = att_value.strip('"')

        for var_name, att_name, att_value in re.findall(variable_pattern, text):
            if var_name not in variable_attributes:
                variable_attributes[var_name] = {}
            variable_attributes[var_name][att_name] = att_value.strip('"')

        return global_attributes, variable_attributes

    @staticmethod
    def parse_data(input_key: str, header_line: int, dims: list[str]) -> xr.Dataset:
        df = pd.read_csv(
            input_key,
            header=header_line,
            parse_dates=True,
            date_format="%Y-%m-%d %H:%M:%S.%f",
        )
        if "time" in dims:
            df["time"] = pd.to_datetime(df["time"])
        df = df.set_index(dims)
        ds = xr.Dataset.from_dataframe(df)
        return ds

    def read(self, input_key: str) -> xr.Dataset:
        dims = self.get_dims_from_filename(input_key)

        lines = Path(input_key).read_text().splitlines()
        header_line_idx = int(lines[0].split("=")[1])  # first line is like header=148

        metadata_text = "\n".join(lines[1:header_line_idx])
        global_attrs, var_attrs = self.parse_attributes(metadata_text)
        ds = self.parse_data(input_key, header_line=header_line_idx, dims=dims)

        ds.attrs = global_attrs
        for var_name, attrs in var_attrs.items():
            ds[var_name].attrs = attrs

        return ds
