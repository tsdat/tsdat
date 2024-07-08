import pytest
import xarray as xr


@pytest.fixture
def sample_2D_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={"time": [0, 1, 2], "height": [1, 10, 100], "depth": [-1, -2, -4]},
        data_vars={
            "timestamp": (
                "time",
                ["2022-03-24 21:43:00", "2022-03-24 21:44:00", "2022-03-24 21:45:00"],
            ),
            "First Data Var": (
                "time",
                [71.4, 71.2, 71.1],
                {"_FillValue": -9999},
            ),
            "Second Data Var": (
                ["time", "height"],
                [[87.8, 71.1, 2.1], [85.4, 72.2, 5.4], [81.5, 65.3, 4.4]],
                {"_FillValue": -9999},
            ),
            "Third Data Var": (
                ["time", "depth"],
                [[12.7, 18.6, 41.2], [8.3, 15.9, 38.5], [9.7, 17.7, 39.1]],
                {"_FillValue": -9999},
            ),
        },
    )
