import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tsdat.transform.adi import AdiTransformer


@pytest.mark.requires_adi
def test_transform():
    input_foo_values = np.arange(24, dtype=np.int32)
    input_qc_foo_values = np.zeros(24, dtype=np.int32)

    # Set one bad value
    input_foo_values[4] = -9999.0
    input_qc_foo_values[4] = 1

    # Data points every hour for one day
    input_dataset = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range(start="2023-02-01", freq="H", periods=24),  # type: ignore
                {"units": "Seconds since 1970-01-01 00:00:00"},
            ),
        },
        data_vars={
            "foo": (
                "time",
                input_foo_values,  # type: ignore
                {
                    "units": "m",
                    "_FillValue": -9999.0,
                },
            ),
            "qc_foo": (
                "time",
                input_qc_foo_values,  # type: ignore
                {
                    "units": "1",
                    "long_name": "Quality check results on variable: foo",
                },
            ),
        },
    )

    # Output data points are at midnight and noon for same day.  12 hour bins, left alignment.
    output_foo_values = np.zeros(2)  # default data type is float64
    output_qc_foo_values = np.zeros(2, dtype=np.int32)  # use ints for qc values
    output_time_values = pd.date_range(
        "2023-02-01", "2023-02-02", periods=2 + 1, inclusive="left"
    )
    output_lower_bound = output_time_values
    output_upper_bound = output_time_values + np.timedelta64(12, "h")
    output_time_bounds_values = np.vstack((output_lower_bound, output_upper_bound)).T

    output_dataset = xr.Dataset(
        coords={
            "time": (
                "time",
                output_time_values,
                {"units": "Seconds since 1970-01-01 00:00:00"},
            )
        },
        data_vars={
            "time_bounds": (
                ("time", "bound"),
                output_time_bounds_values,  # type: ignore
                {
                    "long_name": "Time cell bounds",
                },
            ),
            "foo": (
                "time",
                output_foo_values,  # type: ignore
                {
                    "units": "m",
                    "_FillValue": -9999.0,
                },
            ),
            "qc_foo": (
                "time",
                output_qc_foo_values,  # type: ignore
            ),
        },
    )

    # Do the default transformation, which will be bin averaging in this case.
    # We do not have to set width if bounds variables are provided on the output dataset.
    # Alignment will default to center if not passed.
    transform_parameters = {
        "transformation_type": {"time": "TRANS_AUTO"},
        "alignment": {"time": "LEFT"},
    }

    AdiTransformer().transform(
        "foo", input_dataset, output_dataset, transform_parameters
    )

    print("done")
