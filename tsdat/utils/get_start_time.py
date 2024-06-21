import numpy as np
import pandas as pd
import xarray as xr


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
