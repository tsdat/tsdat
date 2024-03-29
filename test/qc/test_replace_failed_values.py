import numpy as np
import xarray as xr

from tsdat.qc.handlers import RemoveFailedValues
from tsdat.testing import assert_close


def test_replace_failed_values(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    expected["monotonic_var"].data[0] = -9999

    failures = np.array([True, False, False, False])  # type: ignore

    handler = RemoveFailedValues()
    dataset = handler.run(sample_dataset, "monotonic_var", failures)

    assert_close(dataset, expected)
