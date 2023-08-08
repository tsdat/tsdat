import logging
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
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
    expected = np.array([False, False, False, False])
    results = checker.run(sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # 'missing_var' data variable
    expected = np.array([True, True, False, False])
    results = checker.run(sample_dataset, "missing_var")
    assert np.array_equal(results, expected)  # type: ignore

    # 'missing_var' data variable
    expected = np.array([False, True, True, False])
    results = checker.run(sample_dataset, "string_var")
    assert np.array_equal(results, expected)  # type: ignore


def test_monotonic_check(sample_dataset: xr.Dataset):
    new_sample_dataset = xr.concat(
        (sample_dataset, sample_dataset), 
        dim='time')

    # either increasing or decreasing allowed
    checker = CheckMonotonic()
    expected = np.array([False, False, False, False])
    results = checker.run(sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # times must be increasing
    checker = CheckMonotonic(parameters={"require_increasing": True})  # type: ignore
    expected = np.array([
        False, False, False, False, True, True, True, True])
    results = checker.run(new_sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # times must be decreasing
    checker = CheckMonotonic(parameters={"require_decreasing": True})  # type: ignore
    expected = np.array(
        [True, True, True, True, True, True, True, True])
    results = checker.run(new_sample_dataset, "time")
    assert np.array_equal(results, expected)  # type: ignore

    # data variable with non-time data type; sanity check
    checker = CheckMonotonic(parameters={"dim": "time"})  # type: ignore
    expected = np.array([False, False, False, False])
    results = checker.run(sample_dataset, "monotonic_var")
    assert np.array_equal(results, expected)  # type: ignore


def test_check_min_classes(sample_dataset: xr.Dataset):
    var_name = "monotonic_var"
    expected = np.array([True, False, False, False])

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
    expected = np.array([False, False, True, True])

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


def test_monotonic_check_ignores_string_vars(caplog: Any):
    ds = xr.Dataset(
        coords={
            "time": pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
            "dir": ["N", "E", "S", "W"],
        },
        data_vars={
            "wind_speed": (["time", "dir"], np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]))  # type: ignore
        },
    )
    expected = np.array([False, False, False, False])

    with caplog.at_level(logging.WARNING):
        results = CheckMonotonic().run(ds, "dir")  # type: ignore
    assert np.array_equal(results, expected)  # type: ignore
    assert (
        "Variable 'dir' has dtype '<U1', which is currently not supported for monotonicity checks."
        in caplog.text
    )


def test_check_delta_classes(sample_dataset: xr.Dataset):
    var_name = "monotonic_var"
    expected = np.array([False, False, False, True])

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
    expected["monotonic_var"].attrs["ancillary_variables"] = "qc_monotonic_var"
    expected["qc_monotonic_var"].attrs = {
        "long_name": "Quality check results for monotonic_var",
        "units": "1",
        "flag_masks": [1, 2, 4],
        "flag_meanings": ["foo", "bar", "baz"],
        "flag_assessments": ["Bad", "Indeterminate", "Indeterminate"],
        "standard_name": "quality_flag",
    }

    expected["qc_monotonic_var"].data[0] = 1
    expected["qc_monotonic_var"].data[1] = 2
    expected["qc_monotonic_var"].data[2] = 3
    expected["qc_monotonic_var"].data[3] = 4
    test_1_failed = np.array([True, False, True, False])
    test_2_failed = np.array([False, True, True, False])
    test_3_failed = np.array([False, False, False, True])

    dataset = sample_dataset.copy()

    handler = RecordQualityResults(
        parameters={"assessment": "Bad", "meaning": "foo"}  # type: ignore
    )
    dataset = handler.run(dataset, "monotonic_var", test_1_failed)

    handler = RecordQualityResults(
        parameters={"assessment": "Indeterminate", "meaning": "bar"}  # type: ignore
    )
    dataset = handler.run(dataset, "monotonic_var", test_2_failed)

    with pytest.warns(DeprecationWarning):
        handler = RecordQualityResults(
            parameters={
                "bit": 9,  # causes deprecation warning and bit ignored
                "assessment": "Indeterminate",
                "meaning": "baz",
            }  # type: ignore
        )
    dataset = handler.run(dataset, "monotonic_var", test_3_failed)
    assert_close(dataset, expected)


def test_replace_failed_values(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    expected["monotonic_var"].data[0] = -9999

    failures = np.array([True, False, False, False])  # type: ignore

    handler = RemoveFailedValues()
    dataset = handler.run(sample_dataset, "monotonic_var", failures)

    assert_close(dataset, expected)


def test_sortdataset_by_coordinate(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore

    # Sort by time, backwards
    input_dataset: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    input_dataset: xr.Dataset = input_dataset.sortby("time", ascending=False)  # type: ignore

    failures = np.array([True, True, True, True])  # type: ignore

    handler = SortDatasetByCoordinate(parameters=dict(correction="Sorted time data!"))  # type: ignore
    dataset = handler.run(input_dataset, "time", failures)

    assert_close(dataset, expected)
    assert dataset.time.attrs.get("corrections_applied") == ["Sorted time data!"]


def test_fail_pipeline_provides_useful_message(caplog: Any):

    ds = xr.Dataset(
        coords={
            "time": pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
            "dir": ["X", "Y", "Z"],
        },
        data_vars={
            "position": (["time", "dir"], np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]))  # type: ignore
        },
    )

    failures = np.array(
        [[False, False, False], [False, False, False], [False, True, False]]
    )
    with pytest.raises(
        DataQualityError, match=r".*Quality results for variable 'position'.*"
    ) as err:
        _ = FailPipeline().run(ds, "position", failures)

    msg = err.value.args[0]

    assert "Quality results for variable 'position' indicate a fatal error" in msg
    assert "1 / 9 values failed" in msg
    assert "The first failures occur at indexes: [[2, 1]]" in msg
    assert "The corresponding values are: [8]" in msg
