import re
import pytest
import tempfile
from pathlib import Path
from pydantic import ValidationError
from typing import Any, Dict
from test.utils import get_error_message
from tsdat.config.storage import (
    DataReaderConfig,
    DataWriterConfig,
    HandlerRegistryConfig,
    StorageConfig,
)


def test_reader_config_produces_expected_dict():
    reader_dict: Dict[str, Any] = {
        "name": "My Data Reader",
        "classname": "tsdat.io.handlers.readers.NetCDFReader",
    }
    expected_dict: Dict[str, Any] = {
        "name": "My Data Reader",
        "classname": "tsdat.io.handlers.readers.NetCDFReader",
        "parameters": {},
        "regex": "",  # Dynamic default to .* is done at the next level up
    }
    reader_model = DataReaderConfig(**reader_dict)
    assert reader_model.dict() == expected_dict


def test_reader_config_validates_properties():
    reader_dict: Dict[str, Any] = {"regex": []}
    expected_error_msgs = [
        "name\n  field required",
        "classname\n  field required",
        "regex\n  str type expected",
    ]
    with pytest.raises(ValidationError) as error:
        DataReaderConfig(**reader_dict)

    actual_msg = get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_writer_config_produces_expected_dict():
    writer_dict: Dict[str, Any] = {
        "name": "My Data Writer",
        "classname": "tsdat.io.handlers.writers.NetCDFWriter",
    }
    expected_dict: Dict[str, Any] = {
        "name": "My Data Writer",
        "classname": "tsdat.io.handlers.writers.NetCDFWriter",
        "parameters": {},
    }
    writer_model = DataWriterConfig(**writer_dict)
    assert writer_model.dict() == expected_dict


def test_writer_config_validates_properties():
    writer_dict: Dict[str, Any] = {"regex": "abc"}
    expected_error_msgs = [
        "name\n  field required",
        "classname\n  field required",
        "regex\n  extra fields not permitted",
    ]
    with pytest.raises(ValidationError) as error:
        DataWriterConfig(**writer_dict)

    actual_msg = get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_handler_registry_produces_expected_dict():
    registry_dict: Dict[str, Any] = {
        "readers": [
            {
                "classname": "tsdat.io.handlers.readers.NetCDFReader",
                "name": "Read NetCDF",
                "regex": ".*\\.nc",
            },
            {
                "classname": "tsdat.io.handlers.readers.CSVReader",
                "name": "Read CSV",
                "regex": ".*\\.csv",
            },
        ],
        "writers": [
            {
                "classname": "tsdat.io.handlers.writers.NetCDFWriter",
                "name": "Write NetCDF",
            },
            {"classname": "tsdat.io.handlers.writers.CSVWriter", "name": "Write CSV"},
        ],
    }
    expected_dict: Dict[str, Any] = {
        "readers": [
            {
                "classname": "tsdat.io.handlers.readers.NetCDFReader",
                "parameters": {},
                "name": "Read NetCDF",
                "regex": re.compile(".*\\.nc"),
            },
            {
                "classname": "tsdat.io.handlers.readers.CSVReader",
                "parameters": {},
                "name": "Read CSV",
                "regex": re.compile(".*\\.csv"),
            },
        ],
        "writers": [
            {
                "classname": "tsdat.io.handlers.writers.NetCDFWriter",
                "parameters": {},
                "name": "Write NetCDF",
            },
            {
                "classname": "tsdat.io.handlers.writers.CSVWriter",
                "parameters": {},
                "name": "Write CSV",
            },
        ],
    }
    registry = HandlerRegistryConfig(**registry_dict)
    assert registry.dict() == expected_dict


def test_handler_registry_sets_default_regex():
    registry_dict: Dict[str, Any] = {
        "readers": [
            {
                "classname": "tsdat.io.handlers.readers.NetCDFReader",
                "name": "Read NetCDF",
            },
        ],
        "writers": [
            {"classname": "tsdat.io.handlers.writers.CSVWriter", "name": "Write CSV"},
        ],
    }
    registry = HandlerRegistryConfig(**registry_dict)
    assert registry.readers[0].regex == re.compile(".*")  # type: ignore


def test_handler_registry_requires_handler_registration():
    registry_dict: Dict[str, Any] = {"readers": [], "writers": []}
    expected_error_msgs = [
        "readers\n  ensure this value has at least 1 items",
        "writers\n  ensure this value has at least 1 items",
    ]
    with pytest.raises(ValidationError) as error:
        HandlerRegistryConfig(**registry_dict)

    actual_msg = get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_handler_registry_validates_properties():
    registry_dict: Dict[str, Any] = {
        "readers": [
            {
                "name": "duplicate_name",
                "classname": "tsdat.io.handlers.readers.CSVReader",
            },
            {
                "name": "duplicate_name",
                "classname": "tsdat.io.handlers.readers.NetCDFReader",
            },
        ],
        "writers": [
            {
                "name": "duplicate_name",
                "classname": "tsdat.io.handlers.writers.CSVWriter",
            },
            {
                "name": "duplicate_name",
                "classname": "tsdat.io.handlers.writers.NetCDFWriter",
            },
        ],
    }
    expected_error_msgs = [
        "readers\n  readers contains handlers with duplicate names: ['duplicate_name']",
        "writers\n  writers contains handlers with duplicate names: ['duplicate_name']",
    ]
    with pytest.raises(ValidationError) as error:
        HandlerRegistryConfig(**registry_dict)

    actual_msg = get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg

    # After correcting the error it should give another error due to input regex not set
    registry_dict["readers"][0]["name"] = "new_name"
    registry_dict["writers"][0]["name"] = "new_name"
    expected_error_msgs = [
        "If len(readers) > 1 then all handlers should define a 'regex' pattern"
    ]
    with pytest.raises(ValidationError) as error:
        HandlerRegistryConfig(**registry_dict)

    actual_msg = get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_storage_config_produces_expected_yaml():
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.io.storage.FileSystem",
        "parameters": {},
        "registry": {
            "readers": [
                {
                    "classname": "tsdat.io.handlers.readers.CsvReader",
                    "parameters": {},
                    "name": "CSV Reader",
                    "regex": re.compile(r".*\.csv"),
                }
            ],
            "writers": [
                {
                    "classname": "tsdat.io.handlers.writers.NetCDFWriter",
                    "parameters": {},
                    "name": "NetCDF Writer",
                }
            ],
        },
    }
    storage_config_model = StorageConfig.from_yaml(
        Path("test/config/yaml/valid-storage.yaml")
    )
    assert storage_config_model.dict() == expected_dict


def test_storage_config_can_generate_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file = Path(tmpdir) / "storage-schema.json"
        StorageConfig.generate_schema(tmp_file)
        assert tmp_file.exists()
