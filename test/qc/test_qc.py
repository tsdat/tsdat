from typing import Any, Dict
import xarray as xr
import pandas as pd
import numpy as np
from numpy.typing import NDArray
from pytest import fixture

from test.utils import assert_close
from tsdat.qc.checkers import CheckMissing, CheckMonotonic


@fixture
def sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={
            "time": (
                pd.date_range(  # type: ignore
                    "2022-04-13 14:10:00",
                    "2022-04-13 14:40:00",
                    periods=4,
                )
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
                np.array([59, 60, 61, 62], dtype=np.float64),  # type: ignore
            ),
            "string_var": (
                "time",
                np.array(["foo", "", "", "bar"]),  # type: ignore
            ),
        },
    )


def test_missing_check(sample_dataset: xr.Dataset):
    checker = CheckMissing()

    # 'time' coordinate variable
    expected = np.bool8([False, False, False, False])
    results = checker.run(sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # 'missing_var' data variable
    expected = np.bool8([True, True, False, False])
    results = checker.run(sample_dataset, "missing_var")
    assert np.array_equal(results, expected)  # type: ignore

    # 'missing_var' data variable
    expected = np.bool8([False, True, True, False])
    results = checker.run(sample_dataset, "string_var")
    assert np.array_equal(results, expected)  # type: ignore


def test_monotonic_check(sample_dataset: xr.Dataset):

    # either increasing or decreasing allowed
    checker = CheckMonotonic()
    expected = np.bool8([False, False, False, False])
    results = checker.run(sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # times must be increasing
    checker = CheckMonotonic(parameters={"require_increasing": True})  # type: ignore
    expected = np.bool8([False, False, False, False])
    results = checker.run(sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # times must be decreasing
    checker = CheckMonotonic(parameters={"require_decreasing": True})  # type: ignore
    expected = np.bool8([True, True, True, True])
    results = checker.run(sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # data variable with non-time data type; sanity check
    checker = CheckMonotonic(parameters={"dim": "time"})  # type: ignore
    expected = np.bool8([False, False, False, False])
    results = checker.run(sample_dataset, "monotonic_var")
    assert np.array_equal(results, expected)  # type: ignore
