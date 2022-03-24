import pytest
from typing import Any, Dict
from pydantic import ValidationError
from test.utils import get_error_message
from tsdat.config.retrieval import DataReaderConfig


def test_reader_config_produces_expected_dict():
    reader_dict: Dict[str, Any] = {
        "classname": "tsdat.io.handlers.readers.NetCDFReader",
    }
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.io.handlers.readers.NetCDFReader",
        "parameters": {},
        "regex": "",  # Dynamic default to .* is done at the next level up
    }
    reader_model = DataReaderConfig(**reader_dict)
    reader_dict = reader_model.dict()
    assert reader_dict == expected_dict


def test_reader_config_validates_properties():
    reader_dict: Dict[str, Any] = {"regex": []}
    expected_error_msgs = [
        "\nclassname\n  field required",
        "\nregex\n  str type expected",
    ]
    with pytest.raises(ValidationError) as error:
        DataReaderConfig(**reader_dict)

    actual_msg = get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg
