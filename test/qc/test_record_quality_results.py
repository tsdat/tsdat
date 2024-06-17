import logging

import numpy as np
import pytest
import xarray as xr

from tsdat.qc.handlers import RecordQualityResults
from tsdat.testing import assert_close


def test_record_quality_results(
    sample_dataset: xr.Dataset, caplog: pytest.LogCaptureFixture
):
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

    caplog.set_level(logging.WARNING)
    handler = RecordQualityResults(
        parameters={
            "bit": 9,  # causes deprecation warning and bit ignored
            "assessment": "Indeterminate",
            "meaning": "baz",
        }  # type: ignore
    )
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "The 'bit' argument is deprecated" in caplog.records[0].message

    dataset = handler.run(dataset, "monotonic_var", test_3_failed)
    assert_close(dataset, expected)
