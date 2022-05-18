import re
import pytest
from typing import Any, Dict
from pydantic import ValidationError
from tsdat.testing import get_pydantic_error_message
from tsdat.config.retriever import DataReaderConfig, RetrieverConfig
from tsdat.utils import model_to_dict


def test_reader_config_produces_expected_dict():
    reader_dict: Dict[str, Any] = {
        "classname": "tsdat.io.handlers.readers.NetCDFReader",
    }
    expected_dict: Dict[str, Any] = {
        "classname": "tsdat.io.handlers.readers.NetCDFReader",
        "parameters": {},
    }
    reader_model = DataReaderConfig(**reader_dict)
    reader_dict = model_to_dict(reader_model)
    assert reader_dict == expected_dict


def test_reader_config_validates_required_properties():
    reader_dict: Dict[str, Any] = {"regex": []}
    expected_error_msgs = [
        "\nclassname\n  field required",
    ]
    with pytest.raises(ValidationError) as error:
        DataReaderConfig(**reader_dict)

    actual_msg = get_pydantic_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


# def test_retriever_config_validates_required_properties():

#     kwargs: Dict[str, Any] = {
#         "classname": "tsdat.io.retrievers.DefaultRetriever",
#         "parameters": {},
#         "readers": {
#             re.compile(r".*\.csv"): {
#                 "classname": "tsdat.io.readers.CSVReader",
#                 "regex": r".*\.csv",
#             },
#             re.compile(r".*\.nc"): {
#                 "classname": "tsdat.io.readers.NetCDFReader",
#             },
#         },
#     }
#     expected_msg = (
#         "If len(readers) > 1 then all readers should define a 'regex' pattern"
#     )
#     with pytest.raises(ValidationError) as error:
#         RetrieverConfig(**kwargs)

#     actual_msg = get_pydantic_error_message(error)
#     assert expected_msg in actual_msg


def test_retriever_config_can_be_created_without_errors():

    kwargs: Dict[str, Any] = {
        "classname": "tsdat.io.retrievers.DefaultRetriever",
        "parameters": {},
        "readers": {
            re.compile(r".*\.csv"): {"classname": "tsdat.io.readers.CSVReader"},
        },
    }
    RetrieverConfig(**kwargs)

    kwargs: Dict[str, Any] = {
        "classname": "tsdat.io.retrievers.DefaultRetriever",
        "parameters": {},
        "readers": {
            re.compile(r".*\.csv"): {"classname": "tsdat.io.readers.CSVReader"},
            re.compile(r".*\.nc"): {"classname": "tsdat.io.readers.NetCDFReader"},
        },
    }
    RetrieverConfig(**kwargs)
