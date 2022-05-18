import numpy as np
import pandas as pd
import xarray as xr
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Extra
from numpy.typing import NDArray

__all__ = [
    "ParameterizedClass",
    "decode_cf",
    "record_corrections_applied",
    "assign_data",
    "get_start_time",
    "get_start_date_and_time_str",
    "get_filename",
]


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
    datastream_name = dataset.attrs["datastream"]
    start_date, start_time = get_start_date_and_time_str(dataset)
    title = "" if title is None else f".{title}"
    extension = extension if extension.startswith(".") else f".{extension}"
    return f"{datastream_name}.{start_date}.{start_time}{title}{extension}"


# def get_raw_filename(
#     raw_data: Union[xr.Dataset, Dict[str, xr.Dataset]], old_filename: str, config
# ) -> str:
#     """Returns the appropriate raw filename of the raw dataset according to
#     MHKIT-Cloud naming conventions. Uses the config object to parse the
#     start date and time from the raw dataset for use in the new filename.

#     The new filename will follow the MHKIT-Cloud Data standards for raw
#     filenames, ie: `datastream_name.date.time.raw.old_filename`, where the
#     data level used in the datastream_name is `00`.

#     :param raw_data: The raw data as an xarray dataset or a dictionary
#         of form {str: xr.Dataset}.
#     :type raw_data: Union[xr.Dataset, Dict[str, xr.Dataset]]
#     :param old_filename: The name of the original raw file.
#     :type old_filename: str
#     :param config: The Config object used to assist reading time data from
#         the raw_dataset.
#     :type config: Config
#     :return: The standardized filename of the raw file.
#     :rtype: str
#     """
#     original_filename = os.path.basename(old_filename)
#     raw_datastream_name = config.pipeline_definition.input_datastream_name
#     time_var = config.dataset_definition.get_variable("time")

#     if isinstance(raw_data, xr.Dataset):
#         raw_data = {old_filename: raw_data}

#     date_times: List[str] = []
#     for filename, ds in raw_data.items():
#         try:
#             start_date, start_time = DSUtil.get_raw_start_time(ds, time_var)
#             date_time = f"{start_date}.{start_time}"
#             date_times.append(date_time)
#         except KeyError:
#             warnings.warn(
#                 f"Could not retrieve `time` from {filename} with retrieval name {time_var}"
#             )
#     assert len(date_times), f"Failed to create new filename from {old_filename}"

#     start_date_time = min(date_times)
#     return f"{raw_datastream_name}.{start_date_time}.raw.{original_filename}"


# def plot_qc(
#     ds: xr.Dataset, variable_name: str, filename: str = None, **kwargs
# ) -> act.plotting.TimeSeriesDisplay:
#     """Create a QC plot for the given variable.  This is based on the ACT library:
#     https://arm-doe.github.io/ACT/source/auto_examples/plot_qc.html#sphx-glr-source-auto-examples-plot-qc-py

#     We provide a convenience wrapper method for basic QC plots of a variable, but
#     we recommend to use ACT directly and look at their examples for more complex plots
#     like plotting variables in two different datasets.

#     TODO: Depending on use cases, we will likely add more arguments to be able to quickly produce
#     the most common types of QC plots.

#     :param ds: A dataset
#     :type ds: xr.Dataset
#     :param variable_name: The variable to plot
#     :type variable_name: str
#     :param filename: The filename for the image.  Saves the plot as this filename if provided.
#     :type filename: str, optional
#     """
#     datastream_name = DSUtil.get_datastream_name(ds=ds)
#     display = act.plotting.TimeSeriesDisplay(
#         ds, subplot_shape=(2,), ds_name=datastream_name, **kwargs
#     )

#     # Plot temperature data in top plot
#     display.plot(variable_name, subplot_index=(0,))

#     # Plot QC data
#     display.qc_flag_block_plot(variable_name, subplot_index=(1,))

#     if filename:
#         plt.savefig(filename)

#     return display


# def get_raw_start_time(
#     raw_ds: xr.Dataset, time_var_definition: "tsdat.config.VariableDefinition"
# ) -> Tuple[str, str]:
#     """Convenience method to get the start date and time from a raw xarray
#     dataset. This uses `time_var_definition.get_input_name()` as the
#     dataset key for the time variable and additionally uses the input's
#     `Converter` object if applicable.

#     :param raw_ds: A raw dataset (not standardized)
#     :type raw_ds: xr.Dataset
#     :param time_var_definition: The 'time' variable definition from the
#         pipeline config
#     :type time_var_definition: VariableDefinition
#     :return: A tuple of strings representing the first time data point
#         in the dataset.  The first string is the day in 'yyyymmdd' format.
#         The second string is the time in 'hhmmss' format.
#     :rtype: Tuple[str, str]
#     """
#     time_var_name = time_var_definition.get_input_name()
#     time_data = raw_ds[time_var_name].data

#     time64_data = time_var_definition.run_converter(time_data)

#     start_datetime64 = np.min(time64_data)
#     return DSUtil.datetime64_to_string(start_datetime64)

# def datetime64_to_timestamp(variable_data: NDArray[Any]) -> NDArray[np.int64]:
#     """Converts each datetime64 value to a timestamp in same units as
#     the variable (eg., seconds, nanoseconds).

#     :param variable_data: ndarray of variable data
#     :type variable_data: np.ndarray
#     :return: An ndarray of the same shape, with time values converted to
#         long timestamps (e.g., int64)
#     :rtype: np.ndarray
#     """
#     return variable_data.astype(pd.Timestamp).astype(np.int64)  # type: ignore


# def datetime64_to_string(datetime64: np.datetime64) -> Tuple[str, str]:
#     """Convert a datetime64 object to formatted string.

#     :param datetime64: The datetime64 object
#     :type datetime64: Union[np.ndarray, np.datetime64]
#     :return: A tuple of strings representing the formatted date.  The first string is
#         the day in 'yyyymmdd' format.  The second string is the time in 'hhmmss' format.
#     :rtype: Tuple[str, str]
#     """
#     datetime_: datetime.datetime = act.utils.datetime64_to_datetime(datetime64)[0]  # type: ignore
#     return datetime_.strftime("%Y%m%d"), datetime_.strftime("%H%M%S")

# IDEA: Method to print a summary of the list of problems with the data
