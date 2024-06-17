from typing import Tuple
import xarray as xr

from .get_start_time import get_start_time


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
