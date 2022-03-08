from pathlib import Path
import re
from typing import Any, Dict
from tsdat.config.pipeline import PipelineConfig
from tsdat.config.dataset import DatasetConfig
from tsdat.config.storage import StorageConfig
from tsdat.config.quality import QualityConfig


# TEST: PipelineConfig can instantiate a real pipeline object
# TEST: PipelineConfig settings (validate: dataset/quality/storage) disables / enables
# validation checks.
# TEST: PipelineConfig performs appropriate cross-validation of dataset, quality, and
# storage configurations.


def test_pipeline_config_reads_yaml():
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.pipeline.ingest.IngestPipeline",
        "parameters": {},
        "associations": [
            ".*\\.csv",
        ],
        "dataset": {
            "path": "test/config/yaml/valid-dataset.yaml",
            "overrides": {
                "/attrs/location_id": "sgp",
                "/coords/0/attrs/units": "km",
                "/data_vars/0/attrs/new_attribute": "please add this attribute",
            },
        },
        "quality": {
            "path": "test/config/yaml/valid-quality.yaml",
            "overrides": {
                "/managers/0/exclude": [],
            },
        },
        "storage": {
            "classname": "tsdat.io.storage.FileSystem",
            "registry": {
                "input_handlers": [
                    {
                        "classname": "tsdat.io.handlers.CsvReader",
                        "name": "CSV Reader",
                        "regex": ".*\\.csv",
                    }
                ],
                "output_handlers": [
                    {
                        "classname": "tsdat.io.handlers.NetCDFWriter",
                        "name": "NetCDF Writer",
                    }
                ],
            },
        },
        "settings": {
            "validate_dataset_config": True,
            "validate_quality_config": True,
            "validate_storage_config": True,
        },
    }

    model = PipelineConfig.from_yaml(
        Path("test/config/yaml/valid-pipeline.yaml"), validate=False
    )
    model_dict = model.dict(exclude_none=True, by_alias=True)

    assert expected_dict == model_dict


def test_pipeline_config_merges_overrides():
    # Load the linked config files separately
    dataset = DatasetConfig.from_yaml(Path("test/config/yaml/valid-dataset.yaml"))
    quality = QualityConfig.from_yaml(Path("test/config/yaml/valid-quality.yaml"))
    storage = StorageConfig.from_yaml(Path("test/config/yaml/valid-storage.yaml"))

    # Do expected overrides
    dataset.attrs.location_id = "sgp"
    dataset.attrs.datastream = "sgp.example.b1"
    dataset.coords[0].attrs.units = "km"
    dataset.data_vars[0].attrs.new_attribute = "please add this attribute"
    quality.managers[0].exclude = []

    dict_kwargs: Dict[str, Any] = {"exclude_none": True, "by_alias": True}
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.pipeline.ingest.IngestPipeline",
        "parameters": {},
        "associations": [re.compile(r".*\.csv")],
        "settings": {
            "validate_dataset_config": True,
            "validate_quality_config": True,
            "validate_storage_config": True,
        },
        "dataset": dataset.dict(**dict_kwargs),
        "quality": quality.dict(**dict_kwargs),
        "storage": storage.dict(**dict_kwargs),
    }

    # Load everything through the PipelineConfig
    model = PipelineConfig.from_yaml(Path("test/config/yaml/valid-pipeline.yaml"))
    model_dict = model.dict(**dict_kwargs)

    assert expected_dict == model_dict
