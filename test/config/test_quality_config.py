import pytest
import tempfile
from pathlib import Path
from typing import Any, Dict
from pydantic import ValidationError

from tsdat.testing import get_pydantic_error_message
from tsdat.config.quality import (
    ManagerConfig,
    QualityConfig,
)


def test_manager_config_produces_expected_dict():
    qc_dict: Dict[str, Any] = {
        "name": "My Coordinate QC Test",
        "checker": {"classname": "tsdat.qc.checkers.CheckMissing"},
        "handlers": [{"classname": "tsdat.qc.handlers.FailPipeline"}],
        "apply_to": ["COORDS"],
    }
    expected_dict: Dict[str, Any] = {
        "name": "My Coordinate QC Test",
        "checker": {"classname": "tsdat.qc.checkers.CheckMissing", "parameters": {}},
        "handlers": [{"classname": "tsdat.qc.handlers.FailPipeline", "parameters": {}}],
        "apply_to": ["COORDS"],
        "exclude": [],
    }
    qc_model = ManagerConfig(**qc_dict)
    assert qc_model.dict() == expected_dict


def test_manager_config_validates_properties():
    qc_dict: Dict[str, Any] = {
        "checker": {},
        "handlers": [],
        "apply_to": [],
        "exclude": "time",
    }
    expected_error_msgs = [
        "name\n  field required",
        "checker -> classname\n  field required",
        "handlers\n  ensure this value has at least 1 items",
        "apply_to\n  ensure this value has at least 1 items",
        "exclude\n  value is not a valid list",
    ]
    with pytest.raises(ValidationError) as error:
        ManagerConfig(**qc_dict)

    actual_msg = get_pydantic_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_quality_config_produces_expected_dict():
    qc_dict: Dict[str, Any] = {
        "managers": [
            {
                "name": "My Coordinate QC Test",
                "checker": {"classname": "tsdat.qc.checkers.CheckMissing"},
                "handlers": [{"classname": "tsdat.qc.handlers.FailPipeline"}],
                "apply_to": ["COORDS"],
            },
        ]
    }
    expected_dict: Dict[str, Any] = {
        "managers": [
            {
                "name": "My Coordinate QC Test",
                "checker": {
                    "classname": "tsdat.qc.checkers.CheckMissing",
                    "parameters": {},
                },
                "handlers": [
                    {
                        "classname": "tsdat.qc.handlers.FailPipeline",
                        "parameters": {},
                    }
                ],
                "apply_to": ["COORDS"],
                "exclude": [],
            },
        ]
    }
    qc_model = QualityConfig(**qc_dict)
    assert qc_model.dict() == expected_dict


def test_quality_config_manager_names_must_be_unique():
    qc_dict: Dict[str, Any] = {
        "managers": [
            {
                "name": "My Coordinate QC Test",
                "checker": {"classname": "tsdat.qc.checkers.CheckMissing"},
                "handlers": [{"classname": "tsdat.qc.handlers.FailPipeline"}],
                "apply_to": ["COORDS"],
            },
            {
                "name": "My Coordinate QC Test",
                "checker": {"classname": "tsdat.qc.checkers.CheckMissing"},
                "handlers": [{"classname": "tsdat.qc.handlers.FailPipeline"}],
                "apply_to": ["COORDS"],
            },
        ]
    }
    with pytest.raises(
        ValidationError,
        match=r"Duplicate quality manager names found: \['My Coordinate QC Test'\]",
    ):
        QualityConfig(**qc_dict)


def test_quality_config_managers_are_optional():
    qc_dict: Dict[str, Any] = {"managers": []}
    expected: Dict[str, Any] = {"managers": []}
    qc_model = QualityConfig(**qc_dict)
    assert qc_model.dict() == expected


def test_quality_config_from_yaml():
    expected_dict: Dict[str, Any] = {
        "managers": [
            {
                "name": "Require Valid Coordinate Variables",
                "checker": {
                    "classname": "tsdat.qc.checkers.CheckMissing",
                    "parameters": {},
                },
                "handlers": [
                    {
                        "classname": "tsdat.qc.handlers.FailPipeline",
                        "parameters": {},
                    }
                ],
                "apply_to": ["COORDS"],
                "exclude": [],
            },
        ]
    }
    qc_model = QualityConfig.from_yaml(Path("test/config/yaml/quality.yaml"))
    assert qc_model.dict() == expected_dict


def test_quality_config_can_generate_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file = Path(tmpdir) / "quality-schema.json"
        QualityConfig.generate_schema(tmp_file)
        assert tmp_file.exists()
