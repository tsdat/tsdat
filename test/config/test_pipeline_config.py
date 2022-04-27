import re
import tempfile
from pathlib import Path
from typing import Any, Dict
from tsdat.config.pipeline import PipelineConfig
from tsdat.config.dataset import DatasetConfig
from tsdat.config.retriever import RetrieverConfig
from tsdat.config.storage import StorageConfig
from tsdat.config.quality import QualityConfig


def test_pipeline_config_merges_overrides():
    # Load the linked config files separately
    retriever = RetrieverConfig.from_yaml(Path("test/config/yaml/retriever.yaml"))
    dataset = DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))
    quality = QualityConfig.from_yaml(Path("test/config/yaml/quality.yaml"))
    storage = StorageConfig.from_yaml(Path("test/config/yaml/storage.yaml"))

    # Do expected overrides
    dataset.attrs.location_id = "sgp"
    dataset.attrs.datastream = "sgp.example.b1"
    dataset.data_vars["first"].attrs.new_attribute = "please add this attribute"
    quality.managers[0].exclude = []

    dict_kwargs: Dict[str, Any] = {"exclude_none": True, "by_alias": True}
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.pipeline.pipelines.IngestPipeline",
        "parameters": {},
        "triggers": [re.compile(r".*\.csv")],
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
