import numpy as np
import pandas as pd
import pytest
import xarray as xr


@pytest.fixture
def sample_dataset_2d() -> xr.Dataset:
    return xr.Dataset(
        coords={
            "time": pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
            "dir": ["N", "E", "S", "W"],
        },
        data_vars={
            "wind_speed": (["time", "dir"], np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]))  # type: ignore
        },
    )
