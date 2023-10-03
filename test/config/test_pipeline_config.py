import re
import tempfile
from pathlib import Path
from typing import Any, Dict

from tsdat.config.dataset import DatasetConfig
from tsdat.config.pipeline import PipelineConfig
from tsdat.config.quality import QualityConfig
from tsdat.config.retriever import RetrieverConfig
from tsdat.config.storage import StorageConfig
from tsdat.utils import model_to_dict


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

    expected_dict: Dict[str, Any] = {
        "classname": "test.pipeline.examples.Ingest",
        "parameters": {},
        "triggers": [re.compile(r".*\.csv")],
        "retriever": model_to_dict(retriever),
        "dataset": model_to_dict(dataset),
        "quality": model_to_dict(quality),
        "storage": model_to_dict(storage),
    }
    expected_dict["storage"]["parameters"]["data_storage_path"] = "data/{datastream}"

    # Load everything through the PipelineConfig
    pipeline_model = PipelineConfig.from_yaml(Path("test/config/yaml/pipeline.yaml"))
    model_dict = model_to_dict(pipeline_model)

    assert model_dict == expected_dict


def test_pipeline_config_can_generate_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file = Path(tmpdir) / "pipeline-schema.json"
        PipelineConfig.generate_schema(tmp_file)
        assert tmp_file.exists()
