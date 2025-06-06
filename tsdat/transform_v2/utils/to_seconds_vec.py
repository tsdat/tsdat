import numpy as np


def to_seconds_vec(timedelta: np.ndarray) -> np.ndarray:
    return timedelta.astype("timedelta64[ns]") / np.timedelta64(1, "s")
