# TODO: Add UnitsConverter
# TODO: Add StringTimeConverter
# TODO: Determine mechanism for dispatching converters

import act.utils
import numpy as np
import pandas as pd

from abc import ABC, abstractmethod
from typing import Any, Optional
from pydantic import BaseModel, Extra
from tsdat.config.variables import Variable


# TODO: Noticing a lot of classes like:
# BaseConverter(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
# BaseStorage(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
#   ...
# BasePipeline(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
#   ...
# BaseDataHandler(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
#   ...
# and so on... might be about time to standardize these via object inheritance


class BaseConverter(BaseModel, ABC, extra=Extra.forbid):
    parameters: Any = {}

    @abstractmethod
    def run(
        self, data: np.ndarray[Any, Any], variable_config: Variable
    ) -> np.ndarray[Any, Any]:
        ...

    # TODO: Think about refactoring the converter interface; previously these defined a
    # run method with signature (data, in_units, out_units). This worked for the units
    # converter (known as DefaultConverter), but in_units/out_units not used for the
    # StringTimeConverter, nor the TimestampConverter (which, incidentally, is also
    # never used). Seems like the old version basically just used the VariableDefinition
    # to scrape the in/out units and then ran the converters with those. Why not just
    # pass the VariableDefinition (Now VariableConfig) itself and the data, and let the
    # converters figure out what to do with it.


class NoConverter(BaseConverter):
    def run(
        self, data: np.ndarray[Any, Any], variable_config: Variable
    ) -> np.ndarray[Any, Any]:
        return data


class UnitsConverter(BaseConverter):
    def run(
        self, data: np.ndarray[Any, Any], variable_config: Variable
    ) -> np.ndarray[Any, Any]:
        if (
            variable_config.input is None
            or variable_config.input.units is None
            or variable_config.attrs.units is None
            or variable_config.input.units == "1"
            or variable_config.attrs.units == "1"
            or variable_config.input.units == variable_config.attrs.units
        ):
            return data
        return act.utils.data_utils.convert_units(  # type: ignore
            data=data,
            in_units=variable_config.input.units,
            out_units=variable_config.attrs.units,
        )


class StringTimeConverterParameters(BaseModel, extra=Extra.forbid):
    time_format: str
    timezone: Optional[str] = "UTC"
    np_dtype: str = "datetime64[ns]"


class StringTimeConverter(BaseConverter):

    parameters: StringTimeConverterParameters

    def run(
        self, data: np.ndarray[Any, Any], variable_config: Variable
    ) -> np.ndarray[Any, Any]:
        dt = pd.to_datetime(data, format=self.parameters.time_format)  # type: ignore

        if self.parameters.timezone:
            dt = dt.tz_localize(self.parameters.timezone).tz_convert("UTC")  # type: ignore
            # HACK: numpy can't handle localized datetime arrays so we remove the
            # timezone from the datetime array after converting it to UTC
            dt = dt.tz_localize(None)  # type: ignore

        return np.array(dt, dtype=self.parameters.np_dtype)  # type: ignore
