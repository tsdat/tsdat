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
ncconvert to_csv path/to/your/data/*.nc --output-dir path/to/output/folder/ --verbose
```

Format supported other than csv is parquet. To see more information about supported formats, run

```shell
ncconvert --help
```

A python API is also available for each format, e.g.:

```python
import xarray as xr 
from ncconvert import to_csv 

ds = xr.open_dataset("path/to/your/netcdf-file.nc") 

to_csv(ds, "path/to/output/folder/filename.csv") 
```

```python
import xarray as xr
from ncconvert import to_csv_collection

ds = xr.open_dataset("netcdf-file.nc")

# Define the file path where CSV files will be stored
file_path = "path/to/your/output/file"

# Call the to_csv_collection function
csv_files, metadata_file = to_csv_collection(dataset, file_path)
```
