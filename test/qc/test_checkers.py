from typing import Any, Dict, List, Union
import numpy as np
import pytest
import xarray as xr

from tsdat.qc.base import QualityChecker
from tsdat.qc.checkers import (
    CheckFailDelta,
    CheckFailMax,
    CheckFailMin,
    CheckFailRangeMax,
    CheckFailRangeMin,
    CheckMissing,
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


# fmt: off
@pytest.mark.parametrize(
    "checker_class, params, var_name, expected",
    [
        (CheckFailDelta, {}, "monotonic_var", [False, False, False, True]),
        (CheckFailMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckFailMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckFailRangeMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckFailRangeMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckMissing, {}, "missing_var", [True, True, False, False]),
        (CheckMissing, {}, "string_var", [False, True, True, False]),
        (CheckMissing, {}, "time", [False, False, False, False]),
        (CheckMonotonic, {}, "string_var", None),
        (CheckMonotonic, {}, "time", [False, False, False, False]),
        (CheckMonotonic, {"parameters": {"dim": "time"}}, "monotonic_var", [False, False, False, False]),
        (CheckMonotonic, {"parameters": {"dim": "time"}}, "other_var", [False, False, False, True]),
        (CheckMonotonic, {"parameters": {"dim": "time"}}, "other_var_r", [False, True, True, True]),
        (CheckMonotonic, {"parameters": {"require_decreasing": True}}, "time", [False, True, True, True]),
        (CheckMonotonic, {"parameters": {"require_increasing": True}}, "time", [False, False, False, False]),
        (CheckValidDelta, {}, "string_var", None),
        (CheckValidDelta, {"allow_equal": False}, "monotonic_var", [False, False, False, True]),
        (CheckValidMax, {}, "string_var", None),
        (CheckValidMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckValidMin, {}, "string_var", None),
        (CheckValidMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckValidRangeMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckValidRangeMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckWarnDelta, {}, "monotonic_var", [False, False, False, True]),
        (CheckWarnMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckWarnMin, {}, "monotonic_var", [True, False, False, False]),
        (CheckWarnRangeMax, {"allow_equal": False}, "monotonic_var", [False, False, True, True]),
        (CheckWarnRangeMin, {}, "monotonic_var", [True, False, False, False]),
    ],
)
def test_checkers(
        checker_class: QualityChecker,
        params: Dict[str, Any],
        var_name: str,
        expected: Union[List[bool], None],
        sample_dataset: xr.Dataset,
):
    # TODO: Find a different way to test this, calling an ABC is generally considered bad form in Python.
    checker = checker_class(**params)
    failures = checker.run(sample_dataset, var_name)
    if expected is None:
        assert failures is None
    else:
        expected = np.array(expected)
        assert np.array_equal(failures, expected)
# fmt: on
