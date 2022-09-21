#
# Simple python script to generate psuedo-standardized test data for StorageRetriever
# tests.
#

from pathlib import Path
import pandas as pd
import xarray as xr


base_path = Path("test/io/data/retriever-store/data/")

# Sample coordinates
time_3pt = pd.date_range("2022-04-05", "2022-04-06", periods=3 + 1, inclusive="left")  # type: ignore
time_10pt = pd.date_range("2022-04-05", "2022-04-06", periods=10 + 1, inclusive="left")  # type: ignore


#
# First test: Two simple 1D datasets.
#

# First dataset has only 3 points
dataset1 = xr.Dataset(
    coords={"time": time_3pt},
    data_vars={
        "temp": ("time", [71.4, 71.2, 71.1]),
    },
    attrs={"datastream": "humboldt.buoy_z06.a1"},
)

# Second dataset has more data
dataset2 = xr.Dataset(
    coords={"time": time_10pt},  # type: ignore
    data_vars={
        "temp": ("time", [70, 72, 74, 76, 78, 80, 82, 84, 86, 88]),  # type: ignore
        "rh": ("time", [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]),  # type: ignore
    },
    attrs={"datastream": "humboldt.buoy_z07.a1"},
)

dataset1.to_netcdf(base_path / "humboldt.buoy_z06.a1/humboldt.buoy_z06.a1.20220405.000000.nc")  # type: ignore
dataset2.to_netcdf(base_path / "humboldt.buoy_z07.a1/humboldt.buoy_z07.a1.20220405.000000.nc")  # type: ignore

#
# Second test: Multidimensional datasets (time, height)
#

# TODO
# dataset1_2D = xr.Dataset(coords={"time": ("time",)})
