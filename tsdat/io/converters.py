# IDEA: Implement MultiDimensionalGrouper (better name needed. goes from collection of 1D
# variables to one 2D variable)
# IDEA: Use the flyweight pattern to limit memory usage if identical converters would
# be created.

import act # type: ignore
import logging
import xarray as xr
import pandas as pd
import numpy as np
from typing import Any, Dict, Optional
from numpy.typing import NDArray
from pydantic import validator

from ..config.dataset import DatasetConfig
from ..utils import assign_data
from .base import DataConverter

__all__ = ["UnitsConverter", "StringToDatetime"]

logger = logging.getLogger(__name__)


class UnitsConverter(DataConverter):
    """---------------------------------------------------------------------------------
    Converts the units of a retrieved variable to specified output units.

    If the 'input_units' property is set then that string is used to determine the input
    input units, otherwise the converter will attempt to look up and use the 'units'
    attribute on the specified variable in the dataset provided to the `convert` method.
    If the input units cannot be set then a warning is issued and the original dataset
    is returned. The output units are specified by the output dataset configuration.

    Args:
        input_units (Optional[str]): The units that the retrieved data comes in.

    ---------------------------------------------------------------------------------"""

    # IDEA: Validate the literal value of the input units string according to pint
    input_units: Optional[str] = None
    """The units of the input data."""

    def convert(
        self,
        dataset: xr.Dataset,
        dataset_config: DatasetConfig,
        variable_name: str,
        **kwargs: Any,
    ) -> xr.Dataset:
        input_units = self._get_input_units(dataset, variable_name)
        if not input_units:
            logger.warning(
                "Input units for variable '%s' could not be found. Please ensure these"
                " are set either in the retrieval configuration file, or are set on the"
                " 'units' attribute of the '%s' variable in the dataset to converted.",
                variable_name,
                variable_name,
            )
            return dataset

        output_units = dataset_config[variable_name].attrs.units
        if (
            not output_units
            or output_units == "1"
            or output_units == "unitless"
            or input_units == output_units
        ):
            if not output_units:
                logger.warning(
                    "Output units for variable %s could not be found. Please ensure these"
                    " are set in the dataset configuration file for the specified variable.",
                    variable_name,
                )
            return dataset

        data: NDArray[Any] = act.utils.data_utils.convert_units(  # type: ignore
            data=dataset[variable_name].data,
            in_units=input_units,
            out_units=output_units,
        )
        dataset = assign_data(dataset, data, variable_name)
        dataset[variable_name].attrs["units"] = output_units
        logger.debug(
            "Converted '%s's units from '%s' to '%s'",
            variable_name,
            input_units,
            output_units,
        )
        return dataset

    def _get_input_units(self, dataset: xr.Dataset, variable_name: str) -> str:
        units = ""
        if self.input_units:
            units = self.input_units
        elif "units" in dataset[variable_name].attrs:
            units = dataset[variable_name].attrs["units"]
        return units


class StringToDatetime(DataConverter):
    """------------------------------------------------------------------------------------
    Converts date strings into datetime64 data.

    Allows parameters to specify the  string format of the input data, as well as the
    timezone the input data are reported in. If the input timezone is not UTC, the data
    are converted to UTC time.

    Args:
        format (Optional[str]): The format of the string data. See strftime.org for more
            information on what components can be used. If None (the default), then
            pandas will try to interpret the format and convert it automatically. This
            can be unsafe but is not explicitly prohibited, so a warning is issued if
            format is not set explicitly.
        timezone (Optional[str]): The timezone of the input data. If not specified it is
            assumed to be UTC.
        to_datetime_kwargs (Dict[str, Any]): A set of keyword arguments passed to the
            pandas.to_datetime() function as keyword arguments. Note that 'format' is
            already included as a keyword argument. Defaults to {}.

    ------------------------------------------------------------------------------------"""

    format: Optional[str] = None
    """The date format the string is using (e.g., '%Y-%m-%d %H:%M:%S' for date strings
    such as '2022-04-13 23:59:00'), or None (the default) to have pandas guess the
    format automatically."""

    timezone: Optional[str] = None
    """The timezone of the data to convert. If provided, this converter will apply the
    appropriate offset to convert data from the specified timezone to UTC. The timezone
    of the output data is assumed to always be UTC."""

    to_datetime_kwargs: Dict[str, Any] = {}
    """Any parameters set here will be passed to `pd.to_datetime` as keyword
    arguments."""

    @validator("format")
    @classmethod
    def warn_if_no_format_set(cls, format: Optional[str]) -> Optional[str]:
        if not format:
            logger.warning(
                "No string format was provided for the StringToDatetime converter. This"
                " may lead to incorrect behavior in some cases. It is recommended to"
                " set the 'format' parameter explicitly to prevent ambiguous dates."
            )
        return format

    def convert(
        self,
        dataset: xr.Dataset,
        dataset_config: DatasetConfig,
        variable_name: str,
        **kwargs: Any,
    ) -> xr.Dataset:
        dt: NDArray[Any] = dataset[variable_name].data
        dt = pd.to_datetime(dt, format=self.format, **self.to_datetime_kwargs)  # type: ignore

        if self.timezone and self.timezone != "UTC":
            dt = dt.tz_localize(self.timezone).tz_convert("UTC")  # type: ignore
            # Numpy can't handle localized datetime arrays so we force the datetime to
            # be naive (i.e. timezone 'unaware').
            dt = dt.tz_localize(None)  # type: ignore

        dtype = dataset_config[variable_name].dtype
        data: NDArray[Any] = np.array(dt, dtype=dtype)  # type: ignore

        dataset = assign_data(dataset, data, variable_name)

        return dataset
