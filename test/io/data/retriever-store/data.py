#
# Simple python script to generate psuedo-standardized test data for StorageRetriever
# tests.
#

from enum import Enum
import pandas as pd
import xarray as xr
from pathlib import Path

from typer import Typer, Option

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


#
# Second test: Multidimensional datasets (time, height)
#   - tests will take time from dataset 1 and height from dataset 2
#

dataset1_2D = xr.Dataset(
    coords={
        "time": ("time", time_3pt),
        "height": ("height", [1.0, 4.0, 11.0, 17.0], {"units": "m"}),
    },
    data_vars={
        "temp": (
            ("time", "height"),
            [[70, 72, 74, 76], [80, 82, 84, 86], [90, 92, 94, 96]],
        ),
        "humidity": ("time", [0, 10, 20]),
        "pres": ("height", [15, 10, 5, 0]),
    },
    attrs={"datastream": "humboldt.buoy_z06.a1"},
)

dataset2_2D = xr.Dataset(
    coords={
        "time": ("time", time_10pt),
        "height": ("height", [0.0, 5.0, 10.0], {"units": "m"}),
    },
    data_vars={
        "temp": (
            ("time", "height"),
            [
                [0, 2, 4],
                [6, 8, 10],
                [12, 14, 16],
                [18, 20, 22],
                [24, 26, 28],
                [30, 32, 34],
                [36, 38, 40],
                [42, 44, 46],
                [48, 50, 52],
                [54, 56, 58],
            ],
        ),
        "rh": ("time", [40, 45, 50, 55, 60, 65, 70, 75, 80, 85]),
        # "pres": ("height", [14, 12, 10]),
    },
    attrs={"datastream": "humboldt.buoy_z07.a1"},
)

app = Typer()


class DataVersion(str, Enum):
    ONE = "1"
    TWO = "2"


@app.command()
def generate(data_version: DataVersion = Option(...)):
    if data_version == DataVersion.ONE:
        dataset1.to_netcdf(base_path / "humboldt.buoy_z06.a1/humboldt.buoy_z06.a1.20220405.000000.nc")  # type: ignore
        dataset2.to_netcdf(base_path / "humboldt.buoy_z07.a1/humboldt.buoy_z07.a1.20220405.000000.nc")  # type: ignore
    elif data_version == DataVersion.TWO:
        dataset1_2D.to_netcdf(base_path / "humboldt.buoy_z06_2D.a1/humboldt.buoy_z06-2D.a1.20220405.000000.nc")  # type: ignore
        dataset2_2D.to_netcdf(base_path / "humboldt.buoy_z07_2D.a1/humboldt.buoy_z07-2D.a1.20220405.000000.nc")  # type: ignore


if __name__ == "__main__":
    app()
