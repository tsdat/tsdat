from typing import Any
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tsdat.qc.handlers import DataQualityError, FailPipeline


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
