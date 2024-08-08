import pandas as pd
import pytest
import xarray as xr

from tsdat.utils.get_filename import get_filename


@pytest.mark.parametrize(
    ("attrs", "ext", "title", "expected"),
    (
        (dict(datastream="test"), "nc", None, "test.20240808.000000.nc"),
        (dict(datastream="test"), "nc", "title", "test.20240808.000000.title.nc"),
        (dict(datastream="test", title="title"), "nc", None, "test.20240808.000000.nc"),
        (
            dict(datastream="test", title="ds_title"),
            "nc",
            "arg_title",
            "test.20240808.000000.arg_title.nc",
        ),
    ),
)
def test_get_filename(
    attrs: dict[str, str], ext: str, title: str | None, expected: str
):
    dataset = xr.Dataset(
        coords=dict(time=pd.date_range("2024-08-08", "2024-08-09", freq="1h")),
        attrs=attrs,
    )
    assert get_filename(dataset, ext, title) == expected
