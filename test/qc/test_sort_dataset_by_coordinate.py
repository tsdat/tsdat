import numpy as np
import xarray as xr

from tsdat.qc.handlers import SortDatasetByCoordinate
from tsdat.testing import assert_close


def test_sort_dataset_by_coordinate(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore

    # Sort by time, backwards
    input_dataset: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    input_dataset: xr.Dataset = input_dataset.sortby("time", ascending=False)  # type: ignore

    failures = np.array([True, True, True, True])  # type: ignore

    handler = SortDatasetByCoordinate(parameters=dict(correction="Sorted time data!"))  # type: ignore
    dataset = handler.run(input_dataset, "time", failures)

    assert_close(dataset, expected)
    assert dataset.time.attrs.get("corrections_applied") == ["Sorted time data!"]
