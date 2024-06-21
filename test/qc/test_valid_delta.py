import numpy as np
import pandas as pd
import xarray as xr

from tsdat.qc.checkers import CheckValidDelta


def test_valid_delta():
    ds = xr.Dataset(
        coords={
            "time": pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
            "height": np.array([1, 2]),
        },
        data_vars={
            "wind_speed": (["time", "height"], np.array([[10, 20], [11, 25], [16, 31]]), {"units": "m/s", "valid_delta": 5})  # type: ignore
        },
    )
    expected = np.array([[False, False], [False, False], [False, True]])
    results = CheckValidDelta().run(ds, "wind_speed")
    assert np.array_equal(results, expected)  # type: ignore
