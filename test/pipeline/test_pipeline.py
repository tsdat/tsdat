from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tsdat import PipelineConfig, assert_close
from tsdat.pipeline.pipelines import TransformationPipeline


def test_ingest_pipeline():
    expected = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
                {"units": "Seconds since 1970-01-01 00:00:00"},
            ),
        },
        data_vars={
            "first": (
                "time",
                (np.array([71.4, 71.2, 71.1]) - 32) * 5 / 9,  # type: ignore
                {
                    "units": "degC",
                    "_FillValue": -9999.0,
                    "new_attribute": "please add this attribute",
                },
            ),
            "pi": 3.14159,
        },
        attrs={
            "title": "title",
            "description": "description",
            "Conventions": "CF-1.6",
            "featureType": "timeSeries",
            "location_id": "sgp",  # override from the pipeline
            "dataset_name": "example",
            "data_level": "b1",
            "datastream": "sgp.example.b1",
            "inputs": "test/io/data/input.csv",
        },
    )
    expected["pi"].attrs["units"] = "1"

    config = PipelineConfig.from_yaml(Path("test/config/yaml/pipeline.yaml"))
    pipeline = config.instantiate_pipeline()
    dataset = pipeline.run(["test/io/data/input.csv"])

    # Dataset returned by pipeline
    assert_close(dataset, expected)
    assert dataset.attrs["code_version"]
    assert dataset.attrs["history"]
    assert (
        dataset["first"].encoding.get("_FillValue", None) == -9999
        or dataset["first"].attrs.get("_FillValue", None) == -9999
    )

    # Dataset saved to disk
    save_path = Path(
        "storage/root/data/sgp.example.b1/sgp.example.b1.20220324.214300.nc"
    )
    assert save_path.exists()
    saved_dataset: xr.Dataset = xr.open_dataset(save_path)  # type: ignore
    assert_close(saved_dataset, expected)
    assert saved_dataset.attrs["code_version"]
    assert saved_dataset.attrs["history"]
    assert (
        saved_dataset["first"].encoding.get("_FillValue", None) == -9999
        or saved_dataset["first"].attrs.get("_FillValue", None) == -9999
    )

    # Plot file saved to disk
    plot_path = Path(
        "storage/root/ancillary/sgp/sgp.example.b1/sgp.example.b1.20220324.214300.example.png"
    )
    assert plot_path.exists()


@pytest.mark.requires_adi
def test_transformation_pipeline():
    expected = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2022-04-05", "2022-04-06", periods=3 + 1, inclusive="left"),  # type: ignore
                {"units": "Seconds since 1970-01-01 00:00:00"},
            )
        },
        data_vars={
            "temperature": (  # degF -> degC
                "time",
                (np.array([70, 76, 84]) - 32) * 5 / 9,
                {"units": "degC", "_FillValue": -9999.0},
            ),
            "humidity": ("time", [0, 30, 70], {"units": "%", "_FillValue": -9999.0}),
        },
        attrs={
            "datastream": "humboldt.buoy.c1",
            "title": "title",
            "description": "description",
            "Conventions": "CF-1.6",
            "featureType": "timeSeries",
            "location_id": "humboldt",
            "dataset_name": "buoy",
            "data_level": "c1",
            "inputs": [
                "humboldt.buoy_z06.a1::20220405.000000::20220406.000000",
                "humboldt.buoy_z07.a1::20220405.000000::20220406.000000",
            ],
        },
    )

    config = PipelineConfig.from_yaml(Path("test/io/yaml/vap-pipeline.yaml"))
    pipeline: TransformationPipeline = config.instantiate_pipeline()

    inputs = ["20220405.000000", "20220406.000000"]

    dataset = pipeline.run(inputs)

    # Dataset returned by pipeline
    assert_close(dataset, expected)
    assert dataset.attrs.get("code_version") is not None
    assert dataset.attrs.get("history") is not None

    # Dataset saved to disk
    save_path = Path(
        "test/io/data/retriever-store/data/humboldt.buoy.c1/humboldt.buoy.c1.20220405.000000.nc"
    )
    assert save_path.exists()
    saved_dataset: xr.Dataset = xr.open_dataset(save_path, decode_cf=True)  # type: ignore
    assert saved_dataset.attrs.get("code_version") is not None
    assert saved_dataset.attrs.get("history") is not None
    assert_close(saved_dataset, expected)
    # assert (
    #     saved_dataset["first"].encoding.get("_FillValue", None) == -9999
    #     or saved_dataset["first"].attrs.get("_FillValue", None) == -9999
    # )
