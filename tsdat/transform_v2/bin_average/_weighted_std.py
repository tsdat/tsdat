import numpy as np


def _weighted_std(
    data: np.ndarray, weights: np.ndarray, weighted_avg: np.ndarray, axis: int
):
    """
    Calculate the weighted standard deviation of the data along a specified axis.
    Args:
        data (np.ndarray): The data array to compute the weighted standard deviation on.
        weights (np.ndarray): The weights to apply to the data.
        weighted_avg (np.ndarray): The pre-computed weighted average of the data.
        axis (int): The axis along which to compute the weighted standard deviation.
    Returns:
        np.ndarray: The weighted standard deviation of the data along the specified axis.
    """
    sum_of_weights = np.nansum(weights, axis=axis)
    expanded_weighted_avg = np.expand_dims(weighted_avg, axis=axis)
    squared_diff = np.square(data - expanded_weighted_avg)
    weighted_squared_diff_sum = np.nansum(weights * squared_diff, axis=axis)

    with np.errstate(invalid="ignore", divide="ignore"):
        weighted_std = np.sqrt(np.divide(weighted_squared_diff_sum, sum_of_weights))
        weighted_std = np.where(sum_of_weights == 0, np.nan, weighted_std)

    return weighted_std
