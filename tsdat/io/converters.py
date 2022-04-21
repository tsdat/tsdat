# IDEA: Implement MultiDimensionalGrouper (better name needed. goes from collection of 1D
# variables to one 2D variable)
# IDEA: Use the flyweight pattern to limit memory usage if identical converters would
# be created.

import act
import logging
import xarray as xr
import pandas as pd
import numpy as np
from typing import Any, Optional
from numpy.typing import NDArray
from pydantic import BaseModel, Extra, validator
from tsdat.config.dataset import DatasetConfig

from tsdat import utils
from .base import DataConverter

logger = logging.getLogger(__name__)


class UnitsConverter(DataConverter):

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
        """-----------------------------------------------------------------------------
        Converts the units of a specific variable in the dataset to the specified output
        units, if possible.

        If the 'input_units' property is set then that string is used to determine the
        input units, otherwise the 'units' attribute on the `dataset[variable_name]`
        DataArray is used. If neither are set then a warning is issued and the original
        dataset is returned.

        If the output units are not set in the dataset config then a warning is issued
        and the original dataset is returned.

        Args:
            dataset (xr.Dataset): The dataset to convert.
            dataset_config (DatasetConfig): The dataset configuration.
            variable_name (str): The name of the variable to convert.

        Returns:
            xr.Dataset: The dataset with the units for the specified variable possibly
            converted to the output format.

        -----------------------------------------------------------------------------"""
        input_units = self._get_input_units(dataset, variable_name)
        if not input_units:
            logger.warn(
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
            logger.warn(
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
        dataset = utils.assign_data(dataset, data, variable_name)
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
    class Parameters(BaseModel, extra=Extra.allow):
        """Any parameters set here will be passed to `pd.to_datetime` as keyword arguments"""

    parameters: Parameters = Parameters()

    format: Optional[str] = None
    """The date format the string is using (e.g., '%Y-%m-%d %H:%M:%S' for date strings
    such as '2022-04-13 23:59:00'), or None (the default) to have pandas guess the
    format automatically."""

    timezone: Optional[str] = None
    """The timezone of the data to convert. If provided, this converter will apply the
    appropriate offset to convert data from the specified timezone to UTC. The timezone
    of the output data is assumed to always be UTC."""

    @validator("format")
    @classmethod
    def warn_if_no_format_set(cls, format: Optional[str]) -> Optional[str]:
        if not format:
            logger.warn(
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
        dt = pd.to_datetime(dt, format=self.format, **self.parameters.dict())  # type: ignore

        if self.timezone and self.timezone != "UTC":
            dt = dt.tz_localize(self.timezone).tz_convert("UTC")  # type: ignore
            # Numpy can't handle localized datetime arrays so we force the datetime to
            # be naive (i.e. timezone 'unaware').
            dt = dt.tz_localize(None)  # type: ignore

        dtype = dataset_config[variable_name].dtype
        data: NDArray[Any] = np.array(dt, dtype=dtype)  # type: ignore

        dataset = utils.assign_data(dataset, data, variable_name)

        return dataset
