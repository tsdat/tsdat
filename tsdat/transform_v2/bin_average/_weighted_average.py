import numpy as np


def _weighted_average(data: np.ndarray, weights: np.ndarray, axis: int) -> np.ndarray:
    weighted_sum = np.nansum(weights * data, axis=axis)
    sum_of_weights = np.nansum(weights, axis=axis)
    with np.errstate(invalid="ignore", divide="ignore"):
        weighted_avg = np.divide(weighted_sum, sum_of_weights)
        weighted_avg = np.where(sum_of_weights == 0, np.nan, weighted_avg)
    return weighted_avg
