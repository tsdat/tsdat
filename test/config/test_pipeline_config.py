from pathlib import Path
import re
from typing import Any, Dict
from tsdat.config.pipeline import PipelineConfig  # , PipelineDefinition
from tsdat.config.dataset import DatasetDefinition
from tsdat.config.storage import StorageConfig
from tsdat.config.quality import QualityDefinition


# The tsdat/config/pipeline.py file supports two different modes:
# 1. PipelineConfig - config objects for dataset, quality, storage, and settings,
# created from paths to files, or dicts of appropriate types.
# 2. PipelineDefinition - pipeline classname/parameters, associations, and
# PipelineConfig, created from path to pipeline config yaml file or dictionary. Also
# provides method to instantiate the pipeline via the classname parameter


# TEST: PipelineConfig
# TEST: PipelineConfig produces expected model for valid input
# TEST: PipelineConfig from_yaml produces expected model
# TEST: PipelineConfig 'from_yaml'
# TEST: PipelineDefinition 'from_yaml' produces expected model (for valid input)


def test_pipeline_config_constructor():
    expected: Dict[str, Any] = {
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
            # "validate_config_files": True,
            "retain_input_files": False,
        },
    }

    model = PipelineConfig.from_yaml(
        Path("test/config/yaml/valid-pipeline.yaml"), validate=False
    )
    model_dict = model.dict(exclude_none=True, by_alias=True)

    assert expected == model_dict


def test_pipeline_config_from_yaml():
    # Load the linked config files separately
    dataset = DatasetDefinition.from_yaml(Path("test/config/yaml/valid-dataset.yaml"))
    quality = QualityDefinition.from_yaml(Path("test/config/yaml/valid-quality.yaml"))
    storage = StorageConfig.from_yaml(Path("test/config/yaml/valid-storage.yaml"))

    # Do expected overrides
    dataset.attrs.location_id = "sgp"
    dataset.coords[0].attrs.units = "km"
    dataset.data_vars[0].attrs.new_attribute = "please add this attribute"
    quality.managers[0].exclude = []

    dict_kwargs: Dict[str, Any] = {"exclude_none": True, "by_alias": True}
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.pipeline.ingest.IngestPipeline",
        "parameters": {},
        "associations": re.compile(r".*\.csv"),
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
