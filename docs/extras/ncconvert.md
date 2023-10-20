# ncconvert

[![main](https://github.com/tsdat/ncconvert/actions/workflows/pytest.yml/badge.svg)](https://github.com/tsdat/ncconvert/actions/workflows/pytest.yml)
[![codecov](https://codecov.io/gh/tsdat/ncconvert/graph/badge.svg?token=39J7Q6G1N8)](https://codecov.io/gh/tsdat/ncconvert)

[`tsdat/ncconvert`](https://github.com/tsdat/ncconvert) is a tool that helps convert netCDF files to other formats.

## Installation

```shell
pip install "ncconvert[cli]"
```

!!! tip

    The `[cli]` part is an optional specifier configured in `ncconvert` that tells `pip` that we also want to install
    `typer` and other dependencies to support command-line usage of `ncconvert`.

## Usage

```shell
ncconvert to_csv data/*.nc --output-dir output_data/ --verbose
```

Formats other than csv are also supported. To see more information about supported formats, run

```shell
ncconvert --help
```

A python API is also available for each format, e.g.:

```python
import xarray as xr
from ncconvert import to_csv

ds = xr.open_dataset("netcdf-file.nc")

to_csv(ds, "output_folder/filename.csv")
```
