import logging
from typing import Any
import xarray as xr

from tsdat.qc.checkers import CheckMonotonic


def test_monotonic_check_ignores_string_vars(
    sample_dataset_2d: xr.Dataset, caplog: Any
):
    with caplog.at_level(logging.WARNING):
        failures = CheckMonotonic().run(sample_dataset_2d, "dir")  # type: ignore
    assert failures is None
    assert (
        "Variable 'dir' has dtype '<U1', which is currently not supported for"
        " monotonicity checks." in caplog.text
    )
