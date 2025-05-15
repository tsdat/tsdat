import numpy as np


def reshape_weights(
    weights: np.ndarray, data_shape: tuple[int, ...], axis: int
) -> np.ndarray:
    new_weights_shape = np.ones(len(data_shape), dtype=int)
    new_weights_shape[axis] = len(weights)
    adjusted_weights = np.reshape(weights, new_weights_shape)
    return adjusted_weights
