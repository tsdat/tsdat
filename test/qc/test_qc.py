import logging
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tsdat.qc.base import QualityChecker
from tsdat.qc.checkers import (
    CheckFailDelta,
    CheckFailRangeMax,
    CheckMissing,
    CheckFailMax,
    CheckFailMin,
    CheckFailRangeMin,
    CheckMonotonic,
    CheckValidDelta,
    CheckValidMax,
    CheckValidMin,
    CheckValidRangeMax,
    CheckValidRangeMin,
    CheckWarnDelta,
    CheckWarnMax,
    CheckWarnMin,
    CheckWarnRangeMax,
    CheckWarnRangeMin,
)
from tsdat.qc.handlers import (
    RecordQualityResults,
    SortDatasetByCoordinate,
    RemoveFailedValues,
    FailPipeline,
    DataQualityError,
)
from tsdat.testing import assert_close


@pytest.fixture
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


@pytest.fixture
def sample_dataset_2D() -> xr.Dataset:
    return xr.Dataset(
        coords={
            "time": pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
            "dir": ["N", "E", "S", "W"],
        },
        data_vars={
            "wind_speed": (["time", "dir"], np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]))  # type: ignore
        },
    )


# fmt: off
@pytest.mark.parametrize(
    "checker_class, params, var_name, expected",
    [
        (CheckMissing, {}, "time", [False, False, False, False]),
        (CheckMissing, {}, "missing_var", [True, True, False, False]),
        (CheckMissing, {}, "string_var", [False, True, True, False]),
        (CheckMonotonic, {}, "time", [False, False, False, False]),
        (CheckMonotonic, {"parameters": {"require_increasing": True}}, "time", [False, False, False, False]),
        (CheckMonotonic, {"parameters": {"require_decreasing": True}}, "time", [False, True, True, True]),
        (CheckMonotonic, {"parameters": {"dim": "time"}}, "monotonic_var", [False, False, False, False]),
        (CheckMonotonic, {"parameters": {"dim": "time"}}, "other_var", [False, False, False, True]),
        (CheckMonotonic, {"parameters": {"dim": "time"}}, "other_var_r", [False, True, True, True]),
        (CheckValidMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckFailMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckWarnMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckValidRangeMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckFailRangeMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckWarnRangeMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckValidMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckFailMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckWarnMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckValidRangeMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckFailRangeMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckWarnRangeMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckValidDelta, {"allow_equal": False}, "monotonic_var", [False, False, False, True]),
        (CheckFailDelta, {}, "monotonic_var", [False, False, False, True]),
        (CheckWarnDelta, {}, "monotonic_var", [False, False, False, True]),
        (CheckMonotonic, {}, "string_var", None),
        (CheckValidMax, {}, "string_var", None),
        (CheckValidMin, {}, "string_var", None),
        (CheckValidDelta, {}, "string_var", None),
    ],
)
def test_checkers(
    checker_class: QualityChecker,
    params: Dict[str, Any],
    var_name: str,
    expected: Union[List[bool], None],
    sample_dataset: xr.Dataset,
):
    checker = checker_class(**params)
    failures = checker.run(sample_dataset, var_name)
    if expected is None:
        assert failures is None
    else:
        expected = np.array(expected)
        assert np.array_equal(failures, expected)
# fmt: on


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


def test_monotonic_check_ignores_string_vars(
    sample_dataset_2D: xr.Dataset, caplog: Any
):
    with caplog.at_level(logging.WARNING):
        failures = CheckMonotonic().run(sample_dataset_2D, "dir")  # type: ignore
    assert failures is None
    assert (
        "Variable 'dir' has dtype '<U1', which is currently not supported for monotonicity checks."
        in caplog.text
    )


def test_monotonic_with_2D_vars(sample_dataset_2D: xr.Dataset, caplog: Any):
    with caplog.at_level(logging.WARNING):
        failures = CheckMonotonic().run(sample_dataset_2D, "wind_speed")
    assert failures is None
    assert (
        "Variable 'wind_speed' has shape '(3, 4)'. 2D variables must provide a 'dim' parameter"
        in caplog.text
    )

    # Regular check
    checker = CheckMonotonic(parameters=CheckMonotonic.Parameters(dim="time"))
    failures = checker.run(sample_dataset_2D, "wind_speed")
    assert not failures.any()

    # # Manipulate the data
    ds = sample_dataset_2D.copy(deep=True)
    ds["wind_speed"].data += np.array([[4, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]])
    checker = CheckMonotonic(parameters=CheckMonotonic.Parameters(dim="time"))
    failures = checker.run(ds, "wind_speed")
    expected = np.array(
        [
            [True, False, False, False],
            [False, False, False, False],
            [False, False, False, False],
        ]
    )
    assert (failures == expected).all()


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


def test_sort_dataset_by_coordinate(sample_dataset: xr.Dataset):
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
