import pytest
import xarray as xr


@pytest.fixture
def sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={"index": [0, 1, 2]},
        data_vars={
            "timestamp": (
                "index",
                ["2022-03-24 21:43:00", "2022-03-24 21:44:00", "2022-03-24 21:45:00"],
            ),
            "First Data Var": (
                "index",
                [71.4, 71.2, 71.1],
                {"_FillValue": -9999},
            ),
        },
    )
