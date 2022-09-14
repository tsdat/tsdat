from pathlib import Path
import numpy as np
import xarray as xr
import pandas as pd
from tsdat import assert_close, PipelineConfig, TransformationPipeline


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
            "location_id": "sgp",  # override from the pipeline
            "dataset_name": "example",
            "data_level": "b1",
            "datastream": "sgp.example.b1",
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
    saved_dataset: xr.Dataset = xr.open_dataset(save_path)  # type: ignore
    assert_close(saved_dataset, expected)
    assert saved_dataset.attrs["code_version"]
    assert saved_dataset.attrs["history"]
    assert (
        saved_dataset["first"].encoding.get("_FillValue", None) == -9999
        or saved_dataset["first"].attrs.get("_FillValue", None) == -9999
    )


def test_transformation_pipeline_sets_retriever_storage():
    config = PipelineConfig.from_yaml(Path("test/config/yaml/vap-pipeline.yaml"))
    pipeline = config.instantiate_pipeline()

    assert isinstance(pipeline, TransformationPipeline)
    assert pipeline.retriever.storage is pipeline.storage
