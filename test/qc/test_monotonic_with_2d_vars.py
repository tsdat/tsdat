import logging
from typing import Any
import numpy as np
import xarray as xr

from tsdat.qc.checkers import CheckMonotonic


def test_monotonic_with_2d_vars(sample_dataset_2d: xr.Dataset, caplog: Any):
    with caplog.at_level(logging.WARNING):
        failures = CheckMonotonic().run(sample_dataset_2d, "wind_speed")
    assert failures is None
    assert (
        "Variable 'wind_speed' has shape '(3, 4)'. 2D variables must provide a 'dim'"
        " parameter" in caplog.text
    )

    # Regular check
    checker = CheckMonotonic(parameters=CheckMonotonic.Parameters(dim="time"))
    failures = checker.run(sample_dataset_2d, "wind_speed")
    assert not failures.any()

    # # Manipulate the data
    ds = sample_dataset_2d.copy(deep=True)
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
