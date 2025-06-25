import numpy as np


def to_seconds_vec(timedelta: np.ndarray) -> np.ndarray:
    """
    Convert a NumPy array of timedelta64 to seconds as a float array.
    Args:
        timedelta (np.ndarray): A NumPy array of timedelta64 values.
    Returns:
        np.ndarray: A NumPy array of float values representing the number of seconds.
    """
    return timedelta.astype("timedelta64[ns]") / np.timedelta64(1, "s")
