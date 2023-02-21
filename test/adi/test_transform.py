import numpy as np
import xarray as xr
import pandas as pd
from tsdat.adi.transform import AdiTransformer


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
                pd.date_range(start='2023-02-01', freq='H', periods=24),  # type: ignore
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
                    "long_name": "Quality check results",
                    "flag_masks": [1],
                    "flag_meanings": ["Value is equal to _FillValue or NaN"],
                    "flag_assessments": ["Bad"]
                },
            ),
        },
    )

    output_foo_values = np.zeros(2)  # default data type is float64
    output_qc_foo_values = np.zeros(2, dtype=np.int32)  # use ints for qc values

    # Data points at midnight and noon for same day
    output_dataset = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2023-02-01", "2023-02-02", periods=2 + 1, inclusive="left"),  # type: ignore
                {"units": "Seconds since 1970-01-01 00:00:00"},
            ),
        },
        data_vars={
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
                {
                    "units": "1",
                    "long_name": "Quality check results",
                    "flag_masks": [1],
                    "flag_meanings": ["Value is equal to _FillValue or NaN"],
                    "flag_assessments": ["Bad"]
                },
            ),
        },
    )

    # Do the default transformation, which will be bin averaging in this case.
    # We MUST always set bin width.  Alignment will default to center
    transform_parameters = {
        "transformation_type": {
            "name": "transformation_type",
            "coordinate_system_defaults": [
                {
                    "dim": "time",
                    "default_value": "TRANS_AUTO"
                }
            ],
            "input_datastream_defaults": [],
            "variables": []
        },
        "width": {
            "name": "width",
            "coordinate_system_defaults": [
                {
                    "dim": "time",
                    "default_value": 43200  # 12 hours
                }
            ],
            "input_datastream_defaults": [],
            "variables": []
        },
        "alignment": {
            "name": "width",
            "coordinate_system_defaults": [
                {
                    "dim": "time",
                    "default_value": 'LEFT'  # Left align
                }
            ],
            "input_datastream_defaults": [],
            "variables": []
        }
    }

    AdiTransformer().transform(input_dataset.foo, input_dataset.qc_foo, output_dataset.foo, output_dataset.qc_foo,
                               transform_parameters)

    print('done')
