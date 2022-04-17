import re
import tempfile
from pathlib import Path
from typing import Any, Dict
from tsdat.config.pipeline import PipelineConfig
from tsdat.config.dataset import DatasetConfig
from tsdat.config.retriever import RetrieverConfig
from tsdat.config.storage import StorageConfig
from tsdat.config.quality import QualityConfig


# TEST: PipelineConfig can instantiate a real pipeline object
# TEST: PipelineConfig settings (validate: dataset/quality/storage) disables / enables
# validation checks.
# TEST: PipelineConfig performs appropriate cross-validation of dataset, quality, and
# storage configurations.


# def test_pipeline_config_reads_raw_yaml():
#     expected_dict: Dict[str, Any] = {
#         "classname": "tsdat.pipeline.ingest.IngestPipeline",
#         "parameters": {},
#         "associations": [
#             ".*\\.csv",
#         ],
#         "dataset": {
#             "path": "test/config/yaml/dataset.yaml",
#             "overrides": {
#                 "/attrs/location_id": "sgp",
#                 "/coords/time/attrs/units": "km",
#                 "/data_vars/first/attrs/new_attribute": "please add this attribute",
#             },
#         },
#         "quality": {
#             "path": "test/config/yaml/quality.yaml",
#             "overrides": {
#                 "/managers/0/exclude": [],
#             },
#         },
#         "storage": {
#             "path": "test/config/yaml/storage.yaml",
#         },
#         "settings": {
#             "validate_dataset_config": True,
#             "validate_quality_config": True,
#             "validate_storage_config": True,
#         },
#     }

#     model = PipelineConfig.from_yaml(
#         Path("test/config/yaml/pipeline.yaml"), validate=False
#     )
#     model_dict = model.dict(exclude_none=True, by_alias=True)

#     assert model_dict == expected_dict


def test_pipeline_config_merges_overrides():
    # Load the linked config files separately
    retriever = RetrieverConfig.from_yaml(Path("test/config/yaml/retriever.yaml"))
    dataset = DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))
    quality = QualityConfig.from_yaml(Path("test/config/yaml/quality.yaml"))
    storage = StorageConfig.from_yaml(Path("test/config/yaml/storage.yaml"))

    # Do expected overrides
    dataset.attrs.location_id = "sgp"
    dataset.attrs.datastream = "sgp.example.b1"
    dataset.coords["time"].attrs.units = "km"
    dataset.data_vars["first"].attrs.new_attribute = "please add this attribute"
    quality.managers[0].exclude = []

    dict_kwargs: Dict[str, Any] = {"exclude_none": True, "by_alias": True}
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.pipeline.ingest.IngestPipeline",
        "parameters": {},
        "associations": [re.compile(r".*\.csv")],
        "settings": {
            "validate_retriever_config": True,
            "validate_dataset_config": True,
            "validate_quality_config": True,
            "validate_storage_config": True,
        },
        "retriever": retriever.dict(**dict_kwargs),
        "dataset": dataset.dict(**dict_kwargs),
        "quality": quality.dict(**dict_kwargs),
        "storage": storage.dict(**dict_kwargs),
    }

    # Load everything through the PipelineConfig
    model = PipelineConfig.from_yaml(Path("test/config/yaml/pipeline.yaml"))
    model_dict = model.dict(**dict_kwargs)

    assert model_dict == expected_dict


def test_pipeline_config_can_generate_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file = Path(tmpdir) / "pipeline-schema.json"
        PipelineConfig.generate_schema(tmp_file)
        assert tmp_file.exists()
