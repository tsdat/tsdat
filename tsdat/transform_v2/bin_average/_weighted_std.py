import numpy as np


def _weighted_std(
    data: np.ndarray, weights: np.ndarray, weighted_avg: np.ndarray, axis: int
):
    sum_of_weights = np.nansum(weights, axis=axis)
    expanded_weighted_avg = np.expand_dims(weighted_avg, axis=axis)
    squared_diff = np.square(data - expanded_weighted_avg)
    weighted_squared_diff_sum = np.nansum(weights * squared_diff, axis=axis)

    with np.errstate(invalid="ignore", divide="ignore"):
        weighted_std = np.sqrt(np.divide(weighted_squared_diff_sum, sum_of_weights))
        weighted_std = np.where(sum_of_weights == 0, np.nan, weighted_std)

    return weighted_std
