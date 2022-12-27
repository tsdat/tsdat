import numpy as np
import pandas as pd
import xarray as xr
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from pydantic import BaseModel, Extra
from numpy.typing import NDArray
from tstring import Template

__all__ = [
    "ParameterizedClass",
    "decode_cf",
    "record_corrections_applied",
    "assign_data",
    "get_start_time",
    "get_start_date_and_time_str",
    "get_filename",
    "get_datastream",
    "DATASTREAM_TEMPLATE",
    "FILENAME_TEMPLATE",
]

DATASTREAM_TEMPLATE = Template(
    "{location_id}.{dataset_name}[-{qualifier}][-{temporal}].{data_level}"
)

FILENAME_TEMPLATE = Template(
    "{datastream}.{start_date}.{start_time}[.{title}].{extension}"
)


class ParameterizedClass(BaseModel, extra=Extra.forbid):
    """------------------------------------------------------------------------------------
    Base class for any class that accepts 'parameters' as an argument.

    Sets the default 'parameters' to {}. Subclasses of ParameterizedClass should override
    the 'parameters' properties to support custom required or optional arguments from
    configuration files.

    ------------------------------------------------------------------------------------"""

    parameters: Any = {}


def _nested_union(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Dict[Any, Any]:
    for k, v in dict1.items():
        if isinstance(v, dict):
            node = dict2.setdefault(k, {})
            _nested_union(v, node)  # type: ignore
        else:
            dict2[k] = v
    return dict2


# Brilliant solution seen here https://stackoverflow.com/a/65363852/15641512
def model_to_dict(model: BaseModel, by_alias: bool = True) -> Dict[Any, Any]:
    """---------------------------------------------------------------------------------
    Converts the model to a dict with unset optional properties excluded.

    Performs a nested union on the dictionaries produced by setting the `exclude_unset`
    and `exclude_none` options to True for the `model.dict()` method. This allows for
    the preservation of explicit `None` values in the yaml, while still filtering out
    values that default to `None`.

    Borrowed approximately from https://stackoverflow.com/a/65363852/15641512.


    Args:
        model (BaseModel): The pydantic model to dict-ify.

    Returns:
        Dict[Any, Any]: The model as a dictionary.

    ---------------------------------------------------------------------------------"""
    return _nested_union(
        model.dict(exclude_unset=True, by_alias=by_alias),
        model.dict(exclude_none=True, by_alias=by_alias),
    )


def decode_cf(dataset: xr.Dataset) -> xr.Dataset:
    """---------------------------------------------------------------------------------
    Wrapper around `xarray.decode_cf()` which handles additional edge cases.

    This helps ensure that the dataset is formatted and encoded correctly after it has
    been constructed or modified. Handles edge cases for units and data type encodings
    on datetime variables.

    Args:
        dataset (xr.Dataset): The dataset to decode.

    Returns:
        xr.Dataset: The decoded dataset.

    ---------------------------------------------------------------------------------"""
    # We have to make sure that time variables do not have units set as attrs, and
    # instead have units set on the encoding or else xarray will crash when trying
    # to save: https://github.com/pydata/xarray/issues/3739
    for variable in dataset.variables.values():
        if (
            np.issubdtype(variable.data.dtype, np.datetime64)  # type: ignore
            and "units" in variable.attrs
        ):
            units = variable.attrs["units"]
            del variable.attrs["units"]
            variable.encoding["units"] = units  # type: ignore

        # If the _FillValue is already encoded, remove it since it can't be overwritten per xarray
        if "_FillValue" in variable.encoding:  # type: ignore
            del variable.encoding["_FillValue"]  # type: ignore

    # Leaving the "dtype" entry in the encoding for datetime64 variables causes a crash
    # when saving the dataset. Not fixed by: https://github.com/pydata/xarray/pull/4684
    ds: xr.Dataset = xr.decode_cf(dataset)  # type: ignore
    for variable in ds.variables.values():
        if variable.data.dtype.type == np.datetime64:  # type: ignore
            if "dtype" in variable.encoding:  # type: ignore
                del variable.encoding["dtype"]  # type: ignore
    return ds


def record_corrections_applied(
    dataset: xr.Dataset, variable_name: str, message: str
) -> None:
    """---------------------------------------------------------------------------------
    Records the message on the 'corrections_applied' attribute.

    Args:
        dataset (xr.Dataset): The corrected dataset.
        variable_name (str): The name of the variable in the dataset.
        message (str): The message to record.

    ---------------------------------------------------------------------------------"""
    variable_attrs = dataset[variable_name].attrs
    corrections: List[str] = variable_attrs.get("corrections_applied", [])
    corrections.append(message)
    variable_attrs["corrections_applied"] = corrections


def assign_data(
    dataset: xr.Dataset, data: NDArray[Any], variable_name: str
) -> xr.Dataset:
    """---------------------------------------------------------------------------------
    Assigns the data to the specified variable in the dataset.

    If the variable exists and it is a data variable, then the DataArray for the
    specified variable in the dataset will simply have its data replaced with the new
    numpy array. If the variable exists and it is a coordinate variable, then the data
    will replace the coordinate data. If the variable does not exist in the dataset then
    a KeyError will be raised.


    Args:
        dataset (xr.Dataset): The dataset where the data should be assigned.
        data (NDArray[Any]): The data to assign.
        variable_name (str): The name of the variable in the dataset to assign data to.

    Raises:
        KeyError: Raises a KeyError if the specified variable is not in the dataset's
            coords or data_vars dictionary.

    Returns:
        xr.Dataset: The dataset with data assigned to it.

    ---------------------------------------------------------------------------------"""
    if variable_name in dataset.data_vars:
        dataset[variable_name].data = data
    elif variable_name in dataset.coords:
        tmp_name = f"__{variable_name}__"
        dataset = dataset.rename_vars({variable_name: tmp_name})

        # TODO: ensure attrs are copied over too
        dataset[variable_name] = xr.zeros_like(dataset[tmp_name], dtype=data.dtype)  # type: ignore
        dataset[variable_name].data[:] = data[:]
        # dataset = dataset.swap_dims({tmp_name: variable_name})  # type: ignore
        dataset = dataset.drop_vars(tmp_name)
        # dataset = dataset.rename_dims(
        #     {tmp_name: variable_name}
        # )  # FIXME: This might drop all dimensions other than the one that was just renamed
    else:
        raise KeyError(
            f"'{variable_name}' must be a coord or a data_var in the dataset to assign"
            " data to it."
        )
    return dataset


def get_start_time(dataset: xr.Dataset) -> pd.Timestamp:
    """---------------------------------------------------------------------------------
    Gets the earliest 'time' value and returns it as a pandas Timestamp.

    Args:
        dataset (xr.Dataset): The dataset whose start time should be retrieved.

    Returns:
        pd.Timestamp: The timestamp of the earliest time value in the dataset.

    ---------------------------------------------------------------------------------"""
    time64: np.datetime64 = np.min(dataset["time"].data)  # type: ignore
    datetime: pd.Timestamp = pd.to_datetime(time64)  # type: ignore
    return datetime


def get_start_date_and_time_str(dataset: xr.Dataset) -> Tuple[str, str]:
    """---------------------------------------------------------------------------------
    Gets the start date and start time strings from a Dataset.

    The strings are formatted using strftime and the following formats:
        - date: "%Y%m%d"
        - time: ""%H%M%S"

    Args:
        dataset (xr.Dataset): The dataset whose start date and time should be retrieved.

    Returns:
        Tuple[str, str]: The start date and time as strings like "YYYYmmdd", "HHMMSS".

    ---------------------------------------------------------------------------------"""
    timestamp = get_start_time(dataset)
    return timestamp.strftime("%Y%m%d"), timestamp.strftime("%H%M%S")


def get_datastream(**global_attrs: str) -> str:
    return DATASTREAM_TEMPLATE.substitute(global_attrs)


def get_filename(
    dataset: xr.Dataset, extension: str, title: Optional[str] = None
) -> str:
    """---------------------------------------------------------------------------------
    Returns the standardized filename for the provided dataset.

    Returns a key consisting of the dataset's datastream, starting date/time, the
    extension, and an optional title. For file-based storage systems this method may be
    used to generate the basename of the output data file by providing extension as
    '.nc', '.csv', or some other file ending type. For ancillary plot files this can be
    used in the same way by specifying extension as '.png', '.jpeg', etc and by
    specifying the title, resulting in files named like
    '<datastream>.20220424.165314.plot_title.png'.

    Args:
        dataset (xr.Dataset): The dataset (used to extract the datastream and starting /
            ending times).
        extension (str): The file extension that should be used.
        title (Optional[str]): An optional title that will be placed between the start
            time and the extension in the generated filename.

    Returns:
        str: The filename constructed from provided parameters.

    ---------------------------------------------------------------------------------"""
    if extension.startswith("."):
        extension = extension[1:]

    start_date, start_time = get_start_date_and_time_str(dataset)
    return FILENAME_TEMPLATE.substitute(
        dataset.attrs,  # type: ignore
        extension=extension,
        title=title,
        start_date=start_date,
        start_time=start_time,
    )


def get_fields_from_dataset(
    dataset: xr.Dataset,
) -> Dict[str, Optional[Union[str, Callable[[], str]]]]:
    def get_time_fmt(fmt: str) -> str:
        return pd.to_datetime(dataset.time.values[0]).strftime(fmt)  # type: ignore

    return dict(
        datastream=dataset.attrs.get("datastream"),
        location_id=dataset.attrs.get("location_id"),
        data_level=dataset.attrs.get("data_level"),
        year=lambda: get_time_fmt("%Y"),
        month=lambda: get_time_fmt("%m"),
        day=lambda: get_time_fmt("%d"),
    )


def get_fields_from_datastream(datastream: str) -> Dict[str, Optional[str]]:
    # assumes datastream = loc.name[-qual][-temp].lvl
    ds_parts = datastream.split(".")
    assert len(ds_parts) == 3

    name_qual_temp = ds_parts[1].split("-")
    assert len(name_qual_temp) <= 3

    return {
        "datastream": datastream,
        "location_id": ds_parts[0],
        "dataset_name": name_qual_temp[0],
        "qualifier": name_qual_temp[1] if len(name_qual_temp) >= 2 else None,
        "temporal": name_qual_temp[2] if len(name_qual_temp) == 3 else None,
        "data_level": ds_parts[2],
    }


# IDEA: Method to print a summary of the list of problems with the data
