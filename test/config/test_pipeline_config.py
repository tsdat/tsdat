from pathlib import Path, PosixPath
import re
from typing import Any, Dict
from tsdat.config.pipeline import PipelineDefinition


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


def test_pipeline_definition_from_yaml_produces_expected_model():
    expected: Dict[str, Any] = {
        "classname": "tsdat.pipeline.ingest.IngestPipeline",
        "parameters": {},
        "associations": [
            re.compile(".*\\.csv"),
        ],
        "config": {
            "dataset": {
                "path": PosixPath("test/config/yaml/valid-dataset.yaml"),
                "overrides": {
                    "/attrs/location_id": "sgp",
                    "/coords/1/attrs/units": "km",
                    "/data_vars/0/attrs/new_attribute": "please add this attribute",
                },
            },
            "quality": {
                "path": PosixPath("test/config/yaml/valid-quality.yaml"),
                "overrides": {
                    "/managers/0/exclude": [],
                },
            },
            "storage": {
                "path": PosixPath("test/config/yaml/valid-storage.yaml"),
                "overrides": {},
            },
            "settings": {
                "persist_inputs": True,
                "delete_inputs": False,
            },
        },
        "settings": {
            "persist_inputs": True,
            "delete_inputs": False,
            "retain_input_files": False,
        },
    }

    model = PipelineDefinition.from_yaml(Path("test/config/yaml/valid-pipeline.yaml"))
    model_dict = model.dict(exclude_none=True, by_alias=True)

    assert expected == model_dict
