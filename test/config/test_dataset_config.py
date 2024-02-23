import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest
from pydantic import ValidationError

from tsdat.config.attributes import AttributeModel, GlobalAttributes
from tsdat.config.dataset import DatasetConfig
from tsdat.config.variables import (
    Coordinate,
    Variable,
    VariableAttributes,
)
from tsdat.testing import get_pydantic_error_message, get_pydantic_warning_message
from tsdat.utils import model_to_dict


def test_fail_if_non_ascii_attrs():
    attrs = [
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
        actual_msg = get_pydantic_error_message(error)
        assert expected_error_msg in actual_msg


def test_fail_if_missing_required_global_attributes():
    attrs: Dict[str, Any] = {}
    expected_error_msgs = [
        "title\n  field required",
        "location_id\n  field required",
        "dataset_name\n  field required",
        "data_level\n  field required",
    ]
    with pytest.raises(ValidationError) as error:
        GlobalAttributes(**attrs)

    actual_msg = get_pydantic_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_fail_if_invalid_global_attributes():
    attrs: Dict[str, Any] = {
        "title": "",  # Too short
        "description": "",  # Too short
        "code_url": "invalid url",
        "Conventions": 1.0,  # Not strictly a string
        "doi": 1.0,  # Not strictly a string
        "institution": 1.0,  # Not strictly a string
        "references": 1.0,  # Not strictly a string
        "location_id": "",  # Too short
        "dataset_name": "a",  # Too short
        "qualifier": "",  # Too short
        "temporal": "A1",  # Doesn't match regex (a-z)
        "data_level": "1234",  # Too long
    }
    expected_error_msgs = [
        "title\n  ensure this value has at least 1 characters",
        "description\n  ensure this value has at least 1 characters",
        "code_url\n  invalid or missing URL scheme",
        "Conventions\n  str type expected",
        "doi\n  str type expected",
        "institution\n  str type expected",
        "references\n  str type expected",
        "location_id\n  ensure this value has at least 1 character",
        "dataset_name\n  ensure this value has at least 2 characters",
        "qualifier\n  ensure this value has at least 1 character",
        "temporal\n  string does not match regex",
        "data_level\n  ensure this value has at most 3 characters",
    ]
    with pytest.raises(ValidationError) as error:
        GlobalAttributes(**attrs)

    actual_msg = get_pydantic_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_warn_if_unexpected_global_attributes(caplog: pytest.LogCaptureFixture):
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

    caplog.set_level(logging.WARNING)
    model_dict = model_to_dict(GlobalAttributes(**attrs))

    assert len(caplog.records) == 2
    assert caplog.records[0].levelname == "WARNING"
    assert (
        "The 'history' attribute should not be set explicitly."
        in caplog.records[0].message
    )
    assert caplog.records[1].levelname == "WARNING"
    assert (
        "The 'code_version' attribute should not be set explicitly."
        in caplog.records[1].message
    )

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
    model_dict = model_to_dict(GlobalAttributes(**attrs))

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
    model_dict = model_to_dict(GlobalAttributes(**attrs))

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
        # "_FillValue": "a",  # Not a float -- no longer needs to be a number
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
        # "_FillValue\n  value is not a valid float",
    ]
    with pytest.raises(ValidationError) as error:
        VariableAttributes(**attrs)

    actual_msg = get_pydantic_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


@pytest.mark.parametrize(
    ("units", "should_warn"),
    (
        ("1", False),
        ("m", False),
        ("m/s", False),
        ("m^3", False),
        ("kg * m^3", False),
        ("kg m^3", False),
        ("invalid units", True),
    ),
)
def test_valid_variable_units(
    units: str, should_warn: bool, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.WARNING)

    VariableAttributes(units=units)  # type: ignore

    if should_warn:
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "WARNING"
        assert caplog.records[0].message.startswith(
            f"'{units}' is not a valid unit or combination of units."
        )
    else:
        assert len(caplog.records) == 0


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
    }
    model_dict = model_to_dict(VariableAttributes(**attrs))
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
    }
    model_dict = model_to_dict(VariableAttributes(**attrs))
    assert expected == model_dict


def test_fail_if_missing_required_variable_properties():
    var: Dict[str, Any] = {}
    expected_error_msgs = [
        "dtype\n  field required",
        "dims\n  field required",
        "attrs\n  field required",
    ]
    with pytest.raises(ValidationError) as error:
        Variable(**var)

    actual_msg = get_pydantic_error_message(error)
    for expected_msg in expected_error_msgs:
        assert expected_msg in actual_msg


def test_fail_if_bad_variable_name():
    # TEST: This test should work at the coords/data_vars level instead of inside a
    # specific variable.
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

        actual_msg = get_pydantic_error_message(error)
        assert expected_error_msg in actual_msg


def test_coordinate_dimensioned_by_itself():
    base_coord: Dict[str, Any] = {
        "name": "my_coordinate",
        "dtype": "float",
        "attrs": {"units": "1", "_FillValue": -9999.0},
    }
    bad_coord: Dict[str, Any] = {"dims": ["some_other_var"]}
    bad_coord.update(base_coord)
    with pytest.raises(
        ValidationError,
        match=r"coord 'my_coordinate' must have dims \['my_coordinate'\]",
    ):
        Coordinate(**bad_coord)

    good_coord: Dict[str, Any] = {"dims": ["my_coordinate"]}
    good_coord.update(base_coord)
    coord = Coordinate(**good_coord)
    assert good_coord == model_to_dict(coord)


# TEST: variable dtype is one of allowed types
# TEST: dataset validation of data variable dimensions matching coordinate variable name
# TEST: dataset validation of data variable, coordinate variable, name uniqueness
# TEST: dataset validation of time as a required coordinate variable


def test_dataset_definition_from_yaml():
    expected: Dict[str, Any] = {
        "attrs": {
            "title": "title",
            "description": "description",
            "Conventions": "CF-1.6",
            "featureType": "timeSeries",
            "location_id": "abc",
            "dataset_name": "example",
            "data_level": "b1",
            "datastream": "abc.example.b1",
            "history": "",
            "code_version": "",
        },
        "coords": {
            "time": {
                "name": "time",
                "dtype": "datetime64[s]",
                "dims": ["time"],
                "attrs": {
                    "units": "Seconds since 1970-01-01 00:00:00",
                },
            }
        },
        "data_vars": {
            "first": {
                "name": "first",
                "dtype": "float",
                "dims": ["time"],
                "attrs": {"units": "degC", "_FillValue": -9999.0},
            },
            "pi": {
                "name": "pi",
                "data": 3.14159,
                "dtype": "float",
                "dims": [],
                "attrs": {"units": "1", "_FillValue": None},
            },
        },
    }

    model = DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))

    model_dict = model_to_dict(model)
    model_dict["attrs"]["code_version"] = ""  # Don't care to check this value

    assert model_dict == expected


def test_dataset_config_can_generate_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file = Path(tmpdir) / "dataset-schema.json"
        DatasetConfig.generate_schema(tmp_file)
        assert tmp_file.exists()
