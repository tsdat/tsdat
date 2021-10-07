import abc
import numpy as np
import pandas as pd

from act.utils import data_utils
from typing import Dict, Union


class Converter(abc.ABC):
    """Base class for converting data arrays from one units to another.
    Users can extend this class if they have a special units conversion
    for their input data that cannot be resolved with the default converter
    classes.

    :param parameters: A dictionary of converter-specific parameters
        which get passed from the pipeline config file.  Defaults to {}
    :type parameters: dict, optional
    """

    def __init__(self, parameters: Union[Dict, None] = None):
        self.parameters = parameters if parameters is not None else dict()

    @abc.abstractmethod
    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        """Convert the input data from in_units to out_units.

        :param data: Data array to be modified.
        :type data: np.ndarray
        :param in_units: Current units of the data array.
        :type in_units: str
        :param out_units: Units to be converted to.
        :type out_units: str
        :return: Data array converted into the new units.
        :rtype: np.ndarray
        """


class DefaultConverter(Converter):
    """Default class for converting units on data arrays.  This class utilizes
    ACT.utils.data_utils.convert_units, and should work for most variables
    except time (see StringTimeConverter and TimestampTimeConverter)
    """

    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        return data_utils.convert_units(data, in_units, out_units)


class StringTimeConverter(Converter):
    """Convert a time string to a np.datetime64, which is needed for xarray.
    This class utilizes pd.to_datetime to perform the conversion.

    One of the parameters should be 'time_format', which is the
    the strftime to parse time, eg "%d/%m/%Y". Note that "%f" will parse all
    the way up to nanoseconds. See strftime documentation for more information on choices.


    :param parameters:  dictionary of converter-specific parameters.  Defaults to {}.
    :type parameters: dict, optional
    """

    def __init__(self, parameters: Union[Dict, None] = None):
        parameters = parameters if parameters is not None else dict()
        super().__init__(parameters=parameters)
        self.format = self.parameters.get("time_format", None)
        assert self.format
        self.timezone = self.parameters.get("timezone", None)

    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        # This returns time that is timezone naive
        datetime_index = pd.to_datetime(data, format=self.format)

        if self.timezone:
            # This adds a timezone for the data, then convert to UTC,
            # then remove the timezone so numpy won't throw a
            # deprecated error.
            datetime_index = datetime_index.tz_localize(self.timezone)
            datetime_index = datetime_index.tz_convert("UTC")
            datetime_index = datetime_index.tz_localize(None)

        # This will convert original data into UTC, correcting for
        # the timezone
        return np.array(datetime_index, np.datetime64)


class TimestampTimeConverter(Converter):
    """Convert a numeric UTC timestamp to a np.datetime64, which is needed for
    xarray.  This class utilizes pd.to_datetime to perform the conversion.

    One of the parameters should be 'unit'. This parameter denotes the time
    unit (e.g., D,s,ms,us,ns), which is an integer or float number. The
    timestamp will be based off the unix epoch start.

    :param parameters: A dictionary of converter-specific parameters which
        get passed from the pipeline config file.  Defaults to {}
    :type parameters: dict, optional
    """

    def __init__(self, parameters: Union[Dict, None] = None):
        parameters = parameters if parameters is not None else dict()
        super().__init__(parameters=parameters)
        self.unit = self.parameters.get("unit", None)
        assert self.unit

    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        return np.array(pd.to_datetime(data, unit=self.unit), np.datetime64)
