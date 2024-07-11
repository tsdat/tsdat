import re
from pathlib import Path

import pandas as pd
import xarray as xr

from ..base import DataReader

GLOBAL_ATTRS = dict[str, str]
VAR_ATTRS = dict[str, dict[str, str]]
DTYPES = dict[str, str]


class A2eCSVReader(DataReader):
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
        n_dims = int(parts[-3][:-1])
        dims = parts[-3 - n_dims : -3]
        return dims

    @staticmethod
    def parse_metadata(text: str) -> tuple[GLOBAL_ATTRS, VAR_ATTRS, DTYPES]:
        global_attributes: dict[str, str] = {}
        variable_attributes: dict[str, dict[str, str]] = {}
        dtypes: dict[str, str] = {}

        metadata_pattern = re.compile(r"^([\w\s]+)=(.+)$", re.MULTILINE)
        variable_pattern = re.compile(r"^([\w\s]+):(\w+)=(.+)$", re.MULTILINE)

        for att_name, att_value in re.findall(metadata_pattern, text):
            global_attributes[att_name] = att_value.strip('"')

        for var_name, att_name, att_value in re.findall(variable_pattern, text):
            att_value = att_value.strip('"')
            if att_name == "dtype":
                dtypes[var_name] = att_value
            elif var_name not in variable_attributes:
                variable_attributes[var_name] = {}
                variable_attributes[var_name][att_name] = att_value

        return global_attributes, variable_attributes, dtypes

    @staticmethod
    def parse_data(
        input_key: str, header_line: int, dims: list[str], dtypes: dict[str, str]
    ) -> xr.Dataset:
        df = pd.read_csv(
            input_key,
            header=header_line,
            parse_dates=True,
            date_format="%Y-%m-%d %H:%M:%S.%f",
            dtype=dtypes,
        )
        df = df.set_index(dims)
        ds = xr.Dataset.from_dataframe(df)
        return ds

    def read(self, input_key: str) -> xr.Dataset:
        dims = self.get_dims_from_filename(input_key)

        lines = Path(input_key).read_text().splitlines()
        header_line_idx = int(lines[0].split("=")[1])  # first line is like header=148

        metadata_text = "\n".join(lines[1:header_line_idx])
        global_attrs, var_attrs, dtypes = self.parse_metadata(metadata_text)
        ds = self.parse_data(
            input_key, header_line=header_line_idx, dims=dims, dtypes=dtypes
        )

        ds.attrs = global_attrs
        for var_name, attrs in var_attrs.items():
            ds[var_name].attrs = attrs

        return ds
