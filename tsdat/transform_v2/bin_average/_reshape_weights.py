import numpy as np


def reshape_weights(
    weights: np.ndarray, data_shape: tuple[int, ...], axis: int
) -> np.ndarray:
    """
    Reshape the weights array to match the data shape along the specified axis.
    Args:
        weights (np.ndarray): The weights to be reshaped.
        data_shape (tuple[int, ...]): The shape of the data array.
        axis (int): The axis along which the weights should be applied.
    Returns:
        np.ndarray: The reshaped weights array.
    """
    new_weights_shape = np.ones(len(data_shape), dtype=int)
    new_weights_shape[axis] = len(weights)
    adjusted_weights = np.reshape(weights, new_weights_shape)
    return adjusted_weights
