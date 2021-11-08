import os
import pytest
import numpy as np
import xarray as xr

import tsdat
from tsdat.utils import DSUtil
from tsdat.constants import ATTS

from tests import (
    NON_MONOTONIC_CSV,
    PROCESSED_NC,
    PIPELINE_ROBUST_CONFIG,
    STORAGE_CONFIG,
    STORAGE_PATH,
)


@pytest.fixture(scope="session", autouse=True)
def pipeline():
    return tsdat.IngestPipeline(PIPELINE_ROBUST_CONFIG, STORAGE_CONFIG)


@pytest.fixture(scope="session", autouse=True)
def config():
    return tsdat.Config.load(PIPELINE_ROBUST_CONFIG)


@pytest.fixture(scope="session", autouse=True)
def dataset():
    return xr.open_dataset(PROCESSED_NC)


@pytest.fixture(scope="session", autouse=True)
def raw_dataset():
    from tsdat.io.filehandlers import FileHandler

    return FileHandler.read(NON_MONOTONIC_CSV)


def test_corrections_are_recorded(dataset):
    DSUtil.record_corrections_applied(
        ds=dataset,
        variable="uninitialized_var",
        correction="Variable was initialized to _FillValue",
    )
    assert ATTS.CORRECTIONS_APPLIED in dataset["uninitialized_var"].attrs


def test_datetime64_is_converted_to_string():
    time_str = "2020-01-01 00:00:00"
    datetime64 = np.datetime64(time_str)
    assert DSUtil.datetime64_to_string(datetime64) == ("20200101", "000000")


def test_datastream_name_retrieved_from_config(dataset, config):
    datastream_name = dataset.attrs.get("datastream_name")
    assert DSUtil.get_datastream_name(config=config) == datastream_name


def test_start_time_is_correct(raw_dataset, dataset, config):
    expected = ("20211001", "000000")
    time_definition = config.dataset_definition.get_variable("time")
    assert DSUtil.get_raw_start_time(raw_dataset, time_definition) == expected
    assert DSUtil.get_start_time(dataset) == expected


def test_end_time_is_correct(raw_dataset, dataset, config):
    expected = ("20211001", "000002")
    time_definition = config.dataset_definition.get_variable("time")
    assert DSUtil.get_raw_end_time(raw_dataset, time_definition) == expected
    assert DSUtil.get_end_time(dataset) == expected


def test_plotting_utilities(dataset):
    expected_filename = "test.SortedDataset.a1.20211001.000000.height.png"
    filename = DSUtil.get_plot_filename(dataset, "height", "png")
    filepath = os.path.join(STORAGE_PATH, "test.SortedDataset.a1", filename)
    assert filename == expected_filename

    assert DSUtil.get_date_from_filename(filepath) == "20211001.000000"

    DSUtil.plot_qc(dataset, "height_out", filepath)

    assert DSUtil.is_image(filepath)
    assert not DSUtil.is_image(PROCESSED_NC)
