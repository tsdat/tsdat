import numpy as np
import pandas as pd


def get_bound_overlaps(
    input_bounds: np.ndarray, output_bounds: np.ndarray
) -> tuple[dict[int, list[int]], dict[int, list[float]], dict[int, list[float]]]:
    """
    Calculates the overlaps, overlap ratios, and distances between input bounds and
    output bounds.

    Args:
        input_bounds (np.ndarray): An array of shape (n, 2) representing the input bounds,
                                   where each row is [start, end].
        output_bounds (np.ndarray): An array of shape (m, 2) representing the output bounds,
                                    where each row is [start, end].

    Returns:
        tuple[dict[int, list[int]], dict[int, list[float]]]: A tuple containing two dictionaries:
            - The first dictionary maps each output bin index to a list of input bin indices
              that overlap with it.
            - The second dictionary maps each output bin index to a list of ratios,
              representing the fraction of the input bin that is covered by the output
              bin.
            - The third dictionary maps each output bin index to a list of distances,
              representing the distance from the output bin center to each input bin
              center at least 50% covered by the output bin.
    """
    # Convert to numerical arrays to calculate bound overlaps. First, to timedelta64.
    # Then break apart into 1D left and right bounds for input and output. Then use
    # pandas to get seconds from the timedeltas. Finally re-combine into bounds. Clunky
    # because pd.to_timedelta() can't handle a 2D array as input.
    if np.issubdtype(input_bounds.dtype, np.datetime64):
        start_time = input_bounds[0, 0]
        _input_deltas = input_bounds - start_time
        _output_deltas = output_bounds - start_time
        input_l_sec = pd.to_timedelta(_input_deltas[:, 0]).total_seconds()
        input_r_sec = pd.to_timedelta(_input_deltas[:, 1]).total_seconds()
        output_l_sec = pd.to_timedelta(_output_deltas[:, 0]).total_seconds()
        output_r_sec = pd.to_timedelta(_output_deltas[:, 1]).total_seconds()
        input_bounds = np.column_stack((input_l_sec, input_r_sec))
        output_bounds = np.column_stack((output_l_sec, output_r_sec))

    bin_idxs, bin_overlaps, bin_distances = _get_bound_overlaps(
        input_bounds=input_bounds,
        output_bounds=output_bounds,
    )
    return bin_idxs, bin_overlaps, bin_distances


def _get_bound_overlaps(
    input_bounds: np.ndarray, output_bounds: np.ndarray
) -> tuple[dict[int, list[int]], dict[int, list[float]], dict[int, list[float]]]:
    # Expand input shape to be (len(inputs), 1)
    input_starts = np.expand_dims(input_bounds[:, 0], axis=-1)
    input_ends = np.expand_dims(input_bounds[:, 1], axis=-1)

    output_starts = output_bounds[:, 0]
    output_ends = output_bounds[:, 1]  # shape is len(output bins)

    input_centers = np.mean(input_bounds, axis=1)
    input_centers_expanded = np.expand_dims(input_centers, axis=1)
    output_centers = np.mean(output_bounds, axis=1)

    # negative means the input is to the left (less than) the output bin center
    # positive means the input is to the right (greater than) the output bin center
    distances = input_centers_expanded - output_centers

    # Calculate the overlaps
    starts = np.maximum(input_starts, output_starts)
    ends = np.minimum(input_ends, output_ends)
    overlaps = np.maximum(0, (ends - starts))

    # Calculate the overlap ratios
    overlap_ratios = overlaps / (input_ends - input_starts)

    # Mask to filter only positive overlaps
    positive_overlap_mask = overlaps > 0

    bin_idxs: dict[int, list[int]] = {}
    bin_overlaps: dict[int, list[float]] = {}
    bin_distances: dict[int, list[float]] = {}

    for j in range(overlap_ratios.shape[1]):
        input_bin_idxs: list[int] = []
        for i in range(overlap_ratios.shape[0]):
            if positive_overlap_mask[i, j]:
                input_bin_idxs.append(i)
        bin_idxs[j] = input_bin_idxs
        bin_overlaps[j] = list(overlap_ratios[input_bin_idxs, j])
        bin_distances[j] = list(distances[input_bin_idxs, j])
    return bin_idxs, bin_overlaps, bin_distances
