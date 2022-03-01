import pytest
from typing import Any, Dict, List
from pydantic import ValidationError
from pathlib import Path

from tsdat.config.dataset import DatasetDefinition
from tsdat.config.attributes import GlobalAttributes, AttributeModel
from tsdat.config.variables import (
    InputVariable,
    VariableAttributes,
    Coordinate,
    Variable,
)


def _get_error_message(error: Any) -> str:
    return error.getrepr().reprcrash.message


def _get_warning_message(warning: Any) -> str:
    warnings: List[str] = [_warning.message.args[0] for _warning in warning.list]
    return "\n".join(warnings)


def test_fail_if_non_ascii_attrs():
    attrs: List[Dict[str, Any]] = [
        {"want some π?": "3.14159"},  # Non-ascii key
        {"measurement": "ºC"},  # Non-ascii for value
    ]
    expected_error_msgs = [
        "'want some π?' contains a non-ascii character.",
        "attr 'measurement' -> 'ºC' contains a non-ascii character.",
    ]
    for attr, expected_error_msg in zip(attrs, expected_error_msgs):
        with pytest.raises(ValidationError) as error:
            AttributeModel(**attr)
        actual_msg = _get_error_message(error)
        assert expected_error_msg in actual_msg


def test_fail_if_missing_required_global_attributes():
    attrs: Dict[str, Any] = {}
    expected_error_msgs = [
        "title\n  field required",
        "description\n  field required",
        "location_id\n  field required",
        "dataset_name\n  field required",
        "data_level\n  field required",
    ]
    with pytest.raises(ValidationError) as error:
        GlobalAttributes(**attrs)

    actual_msg = _get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_fail_if_invalid_global_attributes():
    attrs: Dict[str, Any] = {
        "title": "",  # Too short
        "description": "",  # Too short
        "code_url": "invalid url",
        "conventions": 1.0,  # Not strictly a string
        "doi": 1.0,  # Not strictly a string
        "institution": 1.0,  # Not strictly a string
        "references": 1.0,  # Not strictly a string
        "location_id": "ab",  # Too short
        "dataset_name": "ab",  # Too short
        "qualifier": "ab",  # Too short
        "temporal": "A1",  # Doesn't match regex (a-z)
        "data_level": "1234",  # Too long
    }
    expected_error_msgs = [
        "title\n  ensure this value has at least 1 characters",
        "description\n  ensure this value has at least 1 characters",
        "code_url\n  invalid or missing URL scheme",
        "conventions\n  str type expected",
        "doi\n  str type expected",
        "institution\n  str type expected",
        "references\n  str type expected",
        "location_id\n  ensure this value has at least 3 characters",
        "dataset_name\n  ensure this value has at least 3 characters",
        "qualifier\n  ensure this value has at least 3 characters",
        "temporal\n  string does not match regex",
        "data_level\n  ensure this value has at most 3 characters",
    ]
    with pytest.raises(ValidationError) as error:
        GlobalAttributes(**attrs)

    actual_msg = _get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_warn_if_unexpected_global_attributes():
    attrs: Dict[str, Any] = {
        "title": "Valid Title",
        "description": "Valid description",
        "location_id": "sgp_01",
        "dataset_name": "valid_example",
        "qualifier": "z01",
        "temporal": "5min",
        "data_level": "a00",
        "history": "Raise a warning",  # Should raise a warning and be replaced with ""
        "code_version": "0.0.1",  # Should raise a warning and be replaced
    }
    expected_warning_msgs = [
        "The 'history' attribute should not be set explicitly. The current value of 'Raise a warning' will be ignored.",
        "The 'code_version' attribute should not be set explicitly. The current value of '0.0.1' will be ignored.",
    ]
    with pytest.warns(UserWarning) as warning:
        model_dict = GlobalAttributes(**attrs).dict(exclude_none=True)

    actual_msg = _get_warning_message(warning)
    for expected_msg in expected_warning_msgs:
        assert expected_msg in actual_msg

    assert model_dict["history"] == ""
    assert model_dict["code_version"] != "0.0.1"


def test_valid_model_adds_global_attributes():
    attrs: Dict[str, Any] = {
        "title": "Valid Title",
        "description": "Valid description",
        "location_id": "sgp_01",
        "dataset_name": "valid_example",
        "qualifier": "z01",
        "temporal": "5min",
        "data_level": "a00",
    }
    model_dict = GlobalAttributes(**attrs).dict(exclude_none=True)

    assert model_dict["datastream"] == "sgp_01.valid_example-z01-5min.a00"
    assert model_dict["history"] == ""
    assert model_dict["code_version"] not in ["", "N/A"]


def test_global_attributes_allow_extra():
    attrs: Dict[Any, Any] = {
        "title": "Valid Title",
        "description": "Valid description",
        "location_id": "sgp_01",
        "dataset_name": "valid_example",
        "data_level": "a00",
        "extra_key": "This should be fine.",
        "extra_extra_key": 400,  # This should also be fine
    }
    model_dict = GlobalAttributes(**attrs).dict(exclude_none=True)

    assert model_dict["extra_key"] == "This should be fine."
    assert model_dict["extra_extra_key"] == 400


def test_fail_if_bad_variable_attributes():
    attrs: Dict[Any, Any] = {
        # No units attribute --> "comment" must contain 'Unknown units.'
        "long_name": 1.0,  # Not strictly a string
        "standard_name": 1.0,  # Not strictly a string
        "comment": "Normally valid",  # But units not provided, so must indicate that
        "valid_range": "a",  # Not a list
        "fail_range": ["a", "b"],  # List elements aren't float
        "warn_range": [1.0, 2.0, 3.0],  # Too many elements
        "valid_delta": "a",  # Not a float
        "fail_delta": "a",  # Not a float
        "warn_delta": "a",  # Not a float
        "_FillValue": "a",  # Not a float
    }
    expected_error_msgs = [
        "The 'units' attr is required if known. If the units are not known,",  # ...
        "long_name\n  str type expected",
        "standard_name\n  str type expected",
        "valid_range\n  value is not a valid list",
        "fail_range -> 0\n  value is not a valid float",
        "warn_range\n  ensure this value has at most 2 items",
        "valid_delta\n  value is not a valid float",
        "fail_delta\n  value is not a valid float",
        "warn_delta\n  value is not a valid float",
        "_FillValue\n  value is not a valid float",
    ]
    with pytest.raises(ValidationError) as error:
        VariableAttributes(**attrs)

    actual_msg = _get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


# TEST: variable attributes validate units string (pre-req: validate units)


def test_valid_variable_attrs_adds_fillvalue():
    attrs: Dict[str, Any] = {
        "units": 1,  # Will be cast to str
        "long_name": "Example Variable",
        "comment": "Normally valid",
        "valid_range": [0, 1000],
        "fail_range": [100, 500],
        "warn_range": [200, 300],
        "valid_delta": 45,
        "fail_delta": 35,
        "warn_delta": 15,
    }
    expected: Dict[str, Any] = {
        "units": "1",
        "long_name": "Example Variable",
        "comment": "Normally valid",
        "valid_range": [0, 1000],
        "fail_range": [100, 500],
        "warn_range": [200, 300],
        "valid_delta": 45,
        "fail_delta": 35,
        "warn_delta": 15,
        "_FillValue": -9999,
    }
    model_dict = VariableAttributes(**attrs).dict(exclude_none=True, by_alias=True)
    assert expected == model_dict


def test_variable_attrs_allow_extra():
    attrs: Dict[str, Any] = {
        "units": 1,  # Will be cast to a string
        "extra": "some extra text",
        "another attr": 200,
    }
    expected: Dict[str, Any] = {
        "units": "1",
        "extra": "some extra text",
        "another attr": 200,
        "_FillValue": -9999.0,
    }
    model_dict = VariableAttributes(**attrs).dict(exclude_none=True, by_alias=True)
    assert expected == model_dict


def test_input_var_properties():
    # Input variable requires name
    invar: Dict[str, Any] = {}
    expected_error_msg = "name\n  field required"
    with pytest.raises(ValidationError) as error:
        InputVariable(**invar)
    actual_msg = _get_error_message(error)
    assert expected_error_msg in actual_msg

    # Input variable can be created, and produces expected result
    invar: Dict[str, Any] = {
        "name": "Ínpü† √a®îåßlé Ñ∂mé",  # spaces, strange characters are fine here
        "units": "km",  # doesn't do anything special by itself
        "extra_property": "hi",  # extra fields are allowed
    }
    expected: Dict[str, Any] = {
        "name": "Ínpü† √a®îåßlé Ñ∂mé",
        "required": True,
        "units": "km",
        "extra_property": "hi",
        "converter": {
            "classname": "tsdat.utils.converters.DefaultConverter",
            "parameters": {},
        },
    }
    model_dict = InputVariable(**invar).dict(exclude_none=True, by_alias=True)
    assert expected == model_dict


def test_fail_if_missing_required_variable_properties():
    var: Dict[str, Any] = {}
    expected_error_msgs = [
        "name\n  field required",
        "dtype\n  field required",
        "dims\n  field required",
        "attrs\n  field required",
    ]
    with pytest.raises(ValidationError) as error:
        Variable(**var)

    actual_msg = _get_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_fail_if_bad_variable_name():
    names: List[str] = [
        "ñø_åsçîí",
        "no space",
        "no\\backslash",
        "no|pipe",
        "no+-*&^!@chars",
    ]
    expected_error_msg = "name\n  string does not match regex"
    good_defaults: Dict[str, Any] = {
        "dtype": "int",
        "dims": ["time"],
        "attrs": {"units": "1"},
    }

    for name in names:
        good_defaults.update({"name": name})
        with pytest.raises(ValidationError) as error:
            Variable(**good_defaults)

        actual_msg = _get_error_message(error)
        assert expected_error_msg in actual_msg


def test_variable_retrieval_config():
    base_var: Dict[str, Any] = {
        "name": "variable_name",
        "dims": ["time"],
        "dtype": "float",
        "attrs": {"units": "1"},
    }

    # Variable retrieved from input
    retrieved_var: Dict[str, Any] = {"input": {"name": "input name"}}
    retrieved_var.update(base_var)
    assert Variable(**retrieved_var).is_retrieved

    # Variable data set directly
    static_var: Dict[str, Any] = {"data": [1]}
    static_var.update(base_var)
    assert Variable(**static_var).is_static

    # Variable not retrieved or set directly
    dynamic_var: Dict[str, Any] = {}
    dynamic_var.update(base_var)
    assert Variable(**dynamic_var).is_dynamic

    # Variable cannot be both retrieved and set directly
    bad_var: Dict[str, Any] = {"input": {"name": "input name"}, "data": [1]}
    bad_var.update(base_var)
    with pytest.raises(
        ValidationError, match="cannot be both retrieved from input and set statically"
    ):
        Variable(**bad_var)


# TEST: variable can have either input or data but not both
# TEST: variable dtype is allowed
# TEST: coordinate must be dimensioned by itself
# TEST: dataset definition creation from dict matches expected (by_alias=True)
# TEST: dataset validation of data variable dimensions matching coordinate variable names
# TEST: dataset validation of data variable, coordinate variable, name uniqueness
# TEST: dataset validation of time as a required coordinate variable
# TEST: dataset can create schema?


def test_dataset_definition_from_yaml():
    expected = {
        "attrs": {
            "title": "title",
            "description": "description",
            "location_id": "abc",
            "dataset_name": "example",
            "data_level": "b1",
            "datastream": "abc.example.b1",
            "history": "",
            "code_version": "",
        },
        "coords": [
            {
                "name": "time",
                "input": {
                    "name": "timestamp",
                    "required": True,
                    "converter": {
                        "classname": "tsdat.utils.converters.StringTimeConverter",
                        "parameters": {
                            "timezone": "UTC",
                            "time_format": "%Y-%m-%d %H:%M:%S",
                        },
                    },
                },
                "dtype": "long",
                "dims": ["time"],
                "attrs": {
                    "units": "Time offset from 1970-01-01 00:00:00",
                    "_FillValue": -9999.0,
                },
            }
        ],
        "data_vars": [
            {
                "name": "first",
                "data": [1, 2, 3, 4, 5],
                "dtype": "float",
                "dims": ["time"],
                "attrs": {"units": "degC", "_FillValue": -9999.0},
            },
            {
                "name": "pi",
                "data": [3.14159],
                "dtype": "float",
                "dims": [],
                "attrs": {"units": "1", "_FillValue": -9999.0},
            },
        ],
    }

    model = DatasetDefinition.from_yaml(Path("test/config/yaml/valid-dataset.yaml"))
    model_dict = model.dict(exclude_none=True, by_alias=True)
    model_dict["attrs"]["code_version"] = ""  # Don't care to check this value

    assert expected == model_dict
