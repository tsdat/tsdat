from typing import Any
import numpy as np
from numpy.typing import NDArray


def is_datetime_like(data: NDArray[Any]) -> bool:
    """Checks if the array has a datetime-like dtype (datetime, timedelta, date)"""
    return np.issubdtype(data.dtype, (np.datetime64, np.timedelta64))
