from pathlib import Path
import numpy as np
import xarray as xr
import pandas as pd
from test.utils import assert_close
from tsdat.config.pipeline import PipelineConfig
from tsdat.config.utils import recusive_instantiate
from tsdat.pipeline.ingest import IngestPipeline


def test_ingest_pipeline():
    expected = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
                {"units": "Time offset from 1970-01-01 00:00:00"},
            ),
        },
        data_vars={
            "first": (
                "time",
                (np.array([71.4, 71.2, 71.1]) - 32) * 5 / 9,  # type: ignore
                {"units": "degC", "new_attribute": "please add this attribute"},
            ),
            "qc_first": (
                "time",
                np.array([0, 0, 0], dtype="int"),  # type: ignore
                {"": ""},  # TODO: Add expected attributes
            ),
            # TODO: Add expected 'pi' variable
        },
        attrs={
            "title": "title",
            "description": "description",
            "location_id": "sgp",  # override from the pipeline
            "dataset_name": "example",
            "data_level": "b1",
        },
    )

    # config = PipelineConfig.from_yaml(Path("test/config/yaml/pipeline.yaml"))
    # pipeline: IngestPipeline = recusive_instantiate(config)

    # dataset = pipeline.run(["test/io/data/input.csv"])

    # assert_close(dataset, expected)
    pass
