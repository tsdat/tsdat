import logging
from typing import Any, Dict, Optional

import act  # type: ignore
import numpy as np
import pandas as pd
import xarray as xr
from numpy.typing import NDArray
from pydantic import validator

from ..base import DataConverter, RetrievedDataset
from ...config.dataset import DatasetConfig

logger = logging.getLogger(__name__)


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

    ------------------------------------------------------------------------------------
    """

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
        data: xr.DataArray,
        variable_name: str,
        dataset_config: DatasetConfig,
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        dt: Any = pd.to_datetime(
            data.data,
            format=self.format,
            **self.to_datetime_kwargs,
        )

        if self.timezone and self.timezone != "UTC":
            dt = dt.tz_localize(self.timezone).tz_convert("UTC")  # type: ignore
            # Numpy can't handle localized datetime arrays so we force the datetime to
            # be naive (i.e. timezone 'unaware').
            dt = dt.tz_localize(None)  # type: ignore

        dtype = dataset_config[variable_name].dtype
        converted: NDArray[Any] = np.array(dt, dtype=dtype)  # type: ignore

        if variable_name in dataset_config.coords:
            data_array = xr.DataArray(
                data=converted,
                dims=data.dims,
                coords={variable_name: converted},
                attrs=data.attrs,
                name=variable_name,
            )
        else:
            data_array = xr.DataArray(
                data=converted,
                dims=data.dims,
                coords=data.coords,
                attrs=data.attrs,
                name=variable_name,
            )
        return data_array
