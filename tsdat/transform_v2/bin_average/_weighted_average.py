import numpy as np


def _weighted_average(data: np.ndarray, weights: np.ndarray, axis: int) -> np.ndarray:
    """
    Calculate the weighted average of the data along a specified axis.
    Args:
        data (np.ndarray): The data array to compute the weighted average on.
        weights (np.ndarray): The weights to apply to the data.
        axis (int): The axis along which to compute the weighted average.
    Returns:
        np.ndarray: The weighted average of the data along the specified axis.
    """
    weighted_sum = np.nansum(weights * data, axis=axis)
    sum_of_weights = np.nansum(weights, axis=axis)
    with np.errstate(invalid="ignore", divide="ignore"):
        weighted_avg = np.divide(weighted_sum, sum_of_weights)
        weighted_avg = np.where(sum_of_weights == 0, np.nan, weighted_avg)
    return weighted_avg
