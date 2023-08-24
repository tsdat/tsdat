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
    expected_errors = {
        "name": "missing",
        "checker": "missing",
        "handlers": "too_short",
        "apply_to": "too_short",
        "exclude": "list_type",
    }
    with pytest.raises(ValidationError) as _errors:
        ManagerConfig(**qc_dict)

    errors = _errors.value.errors()
    assert len(errors) == len(expected_errors)
    for error, (err_loc, err_type) in zip(errors, expected_errors.items()):
        assert error["loc"][0] == err_loc
        assert error["type"] == err_type


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
