import re
import pytest
from typing import Any, Dict
from pydantic import ValidationError
from test.utils import get_error_message
from tsdat.config.retriever import DataReaderConfig, RetrieverConfig


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


def test_reader_config_validates_required_properties():
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


def test_retriever_config_validates_required_properties():

    kwargs: Dict[str, Any] = {
        "classname": "tsdat.io.retrievers.DefaultRetriever",
        "parameters": {},
        "readers": {
            "csv": {"classname": "tsdat.io.readers.CSVReader", "regex": r".*\.csv"},
            "netcdf": {
                "classname": "tsdat.io.readers.NetCDFReader",
            },
        },
    }
    expected_msg = (
        "If len(readers) > 1 then all readers should define a 'regex' pattern"
    )
    with pytest.raises(ValidationError) as error:
        RetrieverConfig(**kwargs)

    actual_msg = get_error_message(error)
    assert expected_msg in actual_msg


def test_retriever_config_can_be_created_without_errors():

    kwargs: Dict[str, Any] = {
        "classname": "tsdat.io.retrievers.DefaultRetriever",
        "parameters": {},
        "readers": {
            "csv": {"classname": "tsdat.io.readers.CSVReader"},
        },
    }
    retriever_config = RetrieverConfig(**kwargs)
    assert retriever_config.readers["csv"].regex == re.compile(".*")  # type: ignore

    kwargs: Dict[str, Any] = {
        "classname": "tsdat.io.retrievers.DefaultRetriever",
        "parameters": {},
        "readers": {
            "csv": {"classname": "tsdat.io.readers.CSVReader", "regex": r".*\.csv"},
            "netcdf": {
                "classname": "tsdat.io.readers.NetCDFReader",
                "regex": r".*\.nc",
            },
        },
    }
    retriever_config = RetrieverConfig(**kwargs)
