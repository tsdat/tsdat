import numpy as np
import pandas as pd
import pytest
import xarray as xr


@pytest.fixture
def sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={
            "time": pd.date_range(  # type: ignore
                "2022-04-13 14:10:00",
                "2022-04-13 14:40:00",
                periods=4,
            )
        },
        data_vars={
            "missing_var": (
                "time",
                np.array([-9999, np.nan, 61, 62], dtype=np.float64),  # type: ignore
                {"_FillValue": -9999},
            ),
            "monotonic_var": (
                "time",
                np.array([59, 60, 61, 64], dtype=np.float64),  # type: ignore
                {
                    "valid_min": 60,
                    "fail_min": 60,
                    "warn_min": 60,
                    "valid_max": 61,
                    "fail_max": 61,
                    "warn_max": 61,
                    "valid_range": [60, 61],
                    "fail_range": [60, 61],
                    "warn_range": [60, 61],
                    "valid_delta": 2,
                    "fail_delta": 2,
                    "warn_delta": 2,
                    "_FillValue": -9999,
                },
            ),
            "other_var": (
                "time",
                np.array([59, 60, 61, 58], dtype=np.float64),  # type: ignore
            ),
            "other_var_r": (
                "time",
                np.array([58, 61, 60, 59], dtype=np.float64),  # type: ignore
            ),
            "string_var": (
                "time",
                np.array(["foo", "", "", "bar"]),  # type: ignore
            ),
        },
    )
