import pytest
import xarray as xr


@pytest.fixture
def multi_var_1D_dataset() -> xr.Dataset:
    ds = xr.Dataset(
        coords={
            "time": (
                "time",
                ["2022-04-13 14:10:00", "2022-04-13 14:20:00", "2022-04-13 14:30:00"],
                {"units": "Seconds since 1970-01-01"},
            )
        },
        data_vars={
            "scalar": 10,
            "first": (
                "time",
                [59, 60, 61],
                {"units": "degF", "number": 1},
            ),
            "second": (
                "time",
                [59, 60, 61],
                {"comment": "test case with no units attr"},
            ),
            "temp": (
                "time",
                [59, 60, 61],
                {"units": "degreeF"},
            ),
            "percent": (
                "time",
                [59, 60, 61],
                {"units": ""},
            ),
            "exponent": (
                "time",
                [59, 60, 61],
                {
                    "units": "km s-1",
                },
            ),
        },
        attrs={"title": "example dataset", "number": 3, "array": [0, 1, 2]},
    )
    return ds
