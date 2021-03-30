import abc
import numpy as np
import pandas as pd
from act.utils import data_utils


class Converter(abc.ABC):

    def __init__(self, parameters={}):
        """-------------------------------------------------------------------
        Base class for converting data arrays from one units to another.
        Users can extend this class if they have a special units conversion
        for their input data that cannot be resolved with the default converter
        classes.

        Args:
            parameters(Dict) : A dictionary of converter-specific parameters
        -------------------------------------------------------------------"""
        self.parameters = parameters

    @abc.abstractmethod
    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        """-------------------------------------------------------------------
        Convert the input data from in_units to out_units.

        Args:
            data(np.ndarray) : Data array to be modified.
            in_units(str)    : Current units of the data array.
            out_units(str)   : Units to be converted to.

        Returns:
            data (np.ndarray): Data array converted into the new units.
        -------------------------------------------------------------------"""


class DefaultConverter(Converter):
    """-------------------------------------------------------------------
    Default class for converting units on data arrays.  This class utilizes
    ACT.utils.data_utils.convert_units, and should work for most variables
    except time (see StringTimeConverter and TimestampTimeConverter)
    -------------------------------------------------------------------"""
    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        return data_utils.convert_units(data, in_units, out_units)


class StringTimeConverter(Converter):

    def __init__(self, parameters={}):
        """-------------------------------------------------------------------
        Convert a time string to a np.datetime64, which is needed for xarray.
        This class utilizes pd.to_datetime to perform the conversion.

        Args:
            parameters(Dict) : A dictionary of converter-specific parameters

                time_format  : The strftime to parse time, eg "%d/%m/%Y",
                               note that "%f" will parse all the way up to
                               nanoseconds. See strftime documentation for
                               more information on choices.
        -------------------------------------------------------------------"""
        super().__init__(parameters=parameters)
        self.format = self.parameters.get('time_format', None)
        assert self.format
        self.timezone = self.parameters.get('timezone', None)

    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        # This returns time that is timezone naive
        datetime_index = pd.to_datetime(data, format=self.format)

        if self.timezone:
            # This adds a timezone for the data
            datetime_index = datetime_index.tz_localize(self.timezone)

        # This will convert original data into UTC, correcting for
        # the timezone
        return np.array(datetime_index, np.datetime64)


class TimestampTimeConverter(Converter):
    """-------------------------------------------------------------------
    Convert a numeric UTC timestamp to a np.datetime64, which is needed for
    xarray.  This class utilizes pd.to_datetime to perform the conversion.

    Args:
        parameters(Dict) : A dictionary of converter-specific parameters

            unit         : The unit of the arg (D,s,ms,us,ns) denote the unit,
                           which is an integer or float number. The
                           timestamp will be based off the unix epoch start.
    -------------------------------------------------------------------"""
    def __init__(self, parameters={}):
        super().__init__(parameters=parameters)
        self.unit = self.parameters.get('unit', None)
        assert self.unit

    def run(self, data: np.ndarray, in_units: str, out_units: str) -> np.ndarray:
        return np.array(pd.to_datetime(data, unit=self.unit), np.datetime64)