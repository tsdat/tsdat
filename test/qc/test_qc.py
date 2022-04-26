from typing import Any, Dict
import xarray as xr
import pandas as pd
import numpy as np
from numpy.typing import NDArray
from pytest import fixture
from tsdat.qc.checkers import *
from tsdat.qc.handlers import *
from tsdat.testing import assert_close


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


def test_check_min_classes(sample_dataset: xr.Dataset):
    var_name = "monotonic_var"
    expected = np.bool8([True, False, False, False])

    checker = CheckValidMin()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckFailMin()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckWarnMin()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckValidRangeMin()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckFailRangeMin()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckWarnRangeMin()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore


def test_check_max_classes(sample_dataset: xr.Dataset):
    var_name = "monotonic_var"
    expected = np.bool8([False, False, True, True])

    checker = CheckValidMax(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckFailMax(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckWarnMax(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckValidRangeMax(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckFailRangeMax(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckWarnRangeMax(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore


def test_check_delta_classes(sample_dataset: xr.Dataset):
    var_name = "monotonic_var"
    expected = np.bool8([False, False, False, True])

    checker = CheckValidDelta(allow_equal=False)
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckFailDelta()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore

    checker = CheckWarnDelta()
    results = checker.run(sample_dataset, var_name)
    assert np.array_equal(results, expected)  # type: ignore


def test_record_quality_results(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    expected["qc_monotonic_var"] = xr.full_like(expected["monotonic_var"], fill_value=0)  # type: ignore
    expected["qc_monotonic_var"].data[0] = 1
    expected["monotonic_var"].attrs["ancillary_variables"] = "qc_monotonic_var"
    expected["qc_monotonic_var"].attrs = {
        "long_name": "Quality check results for monotonic_var",
        "units": "1",
        "flag_masks": [1],
        "flag_meanings": ["foo bar"],
        "flag_assessments": ["Bad"],
        "standard_name": "quality_flag",
    }

    failures: NDArray[np.bool8] = np.bool8([True, False, False, False])  # type: ignore

    parameters: Dict[str, Any] = {"bit": 0, "assessment": "bad", "meaning": "foo bar"}
    handler = RecordQualityResults(parameters=parameters)  # type: ignore
    dataset = handler.run(sample_dataset, "monotonic_var", failures)

    assert_close(dataset, expected)


def test_replace_failed_values(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    expected["monotonic_var"].data[0] = -9999

    failures: NDArray[np.bool8] = np.bool8([True, False, False, False])  # type: ignore

    handler = ReplaceFailedValues()
    dataset = handler.run(sample_dataset, "monotonic_var", failures)

    assert_close(dataset, expected)


def test_sortdataset_by_coordinate(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore

    # Sort by time, backwards
    input_dataset: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    input_dataset: xr.Dataset = input_dataset.sortby("time", ascending=False)  # type: ignore

    failures: NDArray[np.bool8] = np.bool8([True, True, True, True])  # type: ignore

    handler = SortDatasetByCoordinate()
    dataset = handler.run(input_dataset, "time", failures)

    assert_close(dataset, expected)
