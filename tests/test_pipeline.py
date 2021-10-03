import os
import pytest
import shutil
import tsdat
import xarray as xr

from tests import (
    NON_MONOTONIC_CSV,
    PIPELINE_INVALID_CONFIG,
    PIPELINE_FAIL_CONFIG,
    PIPELINE_ROBUST_CONFIG,
    STORAGE_CONFIG,
    STORAGE_PATH,
)


def delete_existing_outputs():
    if os.path.isdir(STORAGE_PATH):
        shutil.rmtree(STORAGE_PATH)


def test_yaml_validation():
    delete_existing_outputs()
    with pytest.raises(tsdat.exceptions.DefinitionError):
        tsdat.IngestPipeline(PIPELINE_INVALID_CONFIG, STORAGE_CONFIG)


def test_fail_non_monotonic():
    delete_existing_outputs()
    Pipeline = tsdat.IngestPipeline(PIPELINE_FAIL_CONFIG, STORAGE_CONFIG)
    with pytest.raises(tsdat.exceptions.QCError):
        Pipeline.run(NON_MONOTONIC_CSV)


def test_robust_pipeline():
    delete_existing_outputs()
    Pipeline = tsdat.IngestPipeline(PIPELINE_ROBUST_CONFIG, STORAGE_CONFIG)
    Pipeline.run(NON_MONOTONIC_CSV)

    raw_dir = os.path.join(STORAGE_PATH, "test.SortedDataset.00")
    processed_dir = os.path.join(STORAGE_PATH, "test.SortedDataset.a1")

    assert os.path.isdir(raw_dir) and os.path.isdir(processed_dir)

    processed_file = os.path.join(processed_dir, os.listdir(processed_dir)[0])
    ds: xr.Dataset = xr.open_dataset(processed_file)

    for expected_output_var in ["time", "height_in", "height_out", "dummy_var"]:
        assert expected_output_var in ds.variables

    assert ds["height_out"].attrs["units"] == "km"

    assert (ds["height_in"].data == 1000 * ds["height_out"].data).all()

    assert (ds["qc_dummy_var"].data == 1).all()

    # Check timestamp order was corrected and is increasing
    diff = ds["time"].diff("time").astype(int)
    assert (diff > 0).all()
