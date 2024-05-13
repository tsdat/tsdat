from typing import List

import xarray as xr


def add_inputs_attr(dataset: xr.Dataset, inputs: List[str]) -> None:
    # A len(list)=1 attr doesn't survive round trip, so we keep it a string in that case
    # https://github.com/pydata/xarray/issues/4798
    inputs_attr = inputs if len(inputs) != 1 else inputs[0]
    dataset.attrs["inputs"] = inputs_attr
