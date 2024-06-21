import numpy as np
import pandas as pd
import pytest
import xarray as xr
from numpy.testing import assert_allclose


@pytest.fixture
def input_dataset() -> xr.Dataset:
    # Data points every hour for one day
    time = pd.date_range(start="2023-02-01", freq="h", periods=24)
    range_vals = np.array([0, 1])
    input_foo_values = np.linspace((0, 0), (23, 23), num=24)
    input_qc_foo_values = np.zeros((24, 2), dtype=int)

    input_dataset = xr.Dataset(
        coords={
            "time": ("time", time, {"units": "Seconds since 1970-01-01 00:00:00"}),
            "range": ("range", range_vals, {"units": "km"}),
        },
        data_vars={
            "foo": (
                ("time", "range"),
                input_foo_values,  # type: ignore
                {"units": "m", "_FillValue": -9999.0},
            ),
            "qc_foo": (
                ("time", "range"),
                input_qc_foo_values,  # type: ignore
                {"units": "1", "long_name": "Quality check results on variable: foo"},
            ),
        },
    )
    return input_dataset


def get_output_dataset(include_bounds: bool = True) -> xr.Dataset:
    # Output points are midnight and noon for 1 day. 12hr bins with left alignment
    time = pd.date_range("2023-02-01", "2023-02-02", periods=2 + 1, inclusive="left")
    time_bound = np.vstack((time, time + np.timedelta64(12, "h"))).T

    foo = np.zeros((2, 2), dtype=float)
    qc_foo = np.zeros((2, 2), dtype=int)  # use ints for qc values

    coords = {
        "time": ("time", time, {"units": "Seconds since 1970-01-01 00:00:00"}),
        "range": ("range", [0, 1], {"units": "km"}),
    }
    data_vars = {
        "time_bounds": (
            ("time", "bound"),
            time_bound,
            {"long_name": "Time cell bounds"},
        ),
        "foo": (("time", "range"), foo, {"units": "m", "_FillValue": -9999.0}),
        "qc_foo": (("time", "range"), qc_foo),
    }
    if not include_bounds:
        _ = data_vars.pop("time_bounds")

    output_dataset = xr.Dataset(coords=coords, data_vars=data_vars)
    return output_dataset


@pytest.mark.requires_adi
def test_transform(input_dataset: xr.Dataset):
    from tsdat.transform.adi import AdiTransformer

    # Set one bad value on the input dataset
    input_dataset["foo"].data[4, 0] = -9999.0
    input_dataset["qc_foo"].data[4, 0] = 1

    output_dataset = get_output_dataset(include_bounds=False)

    # Do the default transformation, which will be bin averaging in this case.
    # We do have to set width since we don't have bounds on the output dataset.
    params = {
        "transformation_type": {"time": "TRANS_AUTO", "range": "TRANS_PASSTHROUGH"},
        "alignment": {"time": "LEFT"},
        "width": {"time": 60 * 60 * 12},  # 12hrs
    }
    AdiTransformer().transform("foo", input_dataset, output_dataset, params)

    # Check output foo values; note that first entry is slightly higher than 5.5 due to
    # us inserting a missing value at (4,0), which slightly increases the average
    assert_allclose(output_dataset["foo"], np.array([[5.636364, 5.5], [17.5, 17.5]]))

    # Check qc foo values; note the (0,0) entry represents range0, time0-12 and there is
    # one bad point in that input range, so it gets an indeterminate assessment
    assert_allclose(output_dataset["qc_foo"], np.array([[32, 0], [0, 0]]))  # 2^5=32
    assert "QC_SOME_BAD_INPUTS" in output_dataset["qc_foo"].attrs["flag_meanings"][5]


@pytest.mark.requires_adi
def test_transform_inverted_dims(input_dataset: xr.Dataset):
    from tsdat.transform.adi import AdiTransformer, BadTransformationSettingsError

    # Transform the datasets to be range, time
    output_dataset = get_output_dataset(include_bounds=False)
    input_dataset = input_dataset.transpose("range", "time")
    output_dataset = output_dataset.transpose("range", "time")

    # Do the default transformation, which will be bin averaging in this case.
    # We do not have to set width if bounds variables are provided on the output dataset
    # Alignment will default to center if not passed.
    params = {
        "transformation_type": {"time": "TRANS_AUTO", "range": "TRANS_PASSTHROUGH"},
        "alignment": {"time": "LEFT"},
        "width": {"time": 60 * 60 * 12},  # 12hrs
    }
    with pytest.raises(BadTransformationSettingsError):
        AdiTransformer().transform("foo", input_dataset, output_dataset, params)
