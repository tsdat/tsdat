import numpy as np
import xarray as xr

from ..utils.create_bounds import create_bounds_from_labels
from ..utils.create_empty_dataset import empty_dataset_like
from ..utils.get_bound_overlaps import get_bound_overlaps
from ..utils.get_filtered_data import get_filtered_data
from ..utils.get_input_variables_for_transform import get_input_variables_for_transform
from ._perform_bin_average_qc_checks import perform_bin_average_qc_checks
from ._reshape_weights import reshape_weights
from ._weighted_average import _weighted_average
from ._weighted_std import _weighted_std


def calculate_bin_average(
    input_dataset: xr.Dataset,
    coord_name: str,
    coord_labels: np.ndarray,
    coord_bounds: np.ndarray,
    filter_bad_qc: bool = False,
    add_metrics: bool = True,
) -> xr.Dataset:
    """
    Calculates weighted averages for variables based on overlaps between input and output bounds.

    Args:
        input_dataset (xr.Dataset): The input xarray Dataset containing the variables.
        coord_name (str): The name of the coordinate variable to modify.
        coord_labels (np.ndarray): The new values for the coordinate variable.
        coord_bounds (np.ndarray): The new bounds for the coordinate variable.
        filter_bad_qc (bool): Flag to exclude data flagged as Bad from the average.
        add_metrics (bool): Flag to add metrics (std deviation, goodfrac %).

    Returns:
        xr.Dataset: The new xarray Dataset averaged across the new coordinate bounds.
    """
    if filter_bad_qc or add_metrics:
        input_dataset = input_dataset.copy()
        input_dataset.clean.cleanup()  # basically required for act QC functions to work

    input_data_variables = get_input_variables_for_transform(input_dataset, coord_name)

    output_dataset = empty_dataset_like(
        input_dataset=input_dataset,
        coord_name=coord_name,
        coord_values=coord_labels,
        coord_bounds=coord_bounds,
        add_transform_qc=True,
        add_metric_vars=add_metrics,
    )
    # TODO: should warn if the bounds if not present and create center-aligned bounds.
    if f"{coord_name}_bounds" in input_dataset:
        input_coord_bounds = input_dataset[f"{coord_name}_bounds"].values
    else:
        input_coord_bounds = create_bounds_from_labels(
            input_dataset[coord_name].values, alignment="center"
        )

    input_indices, overlap_ratios, _ = get_bound_overlaps(
        input_coord_bounds, coord_bounds
    )

    for var_name, data_array in input_data_variables.items():
        axis = data_array.dims.index(coord_name)
        data_values = data_array.values

        _, ind_mask = get_filtered_data(input_dataset, var_name, "Indeterminate")
        filtered_values, bad_mask = get_filtered_data(input_dataset, var_name, "Bad")
        # NOTE: ADI includes indeterminate QC values in what it calls the "good" mask
        good_mask = np.logical_not(bad_mask)

        if filter_bad_qc:
            data_values = filtered_values

        for output_idx, input_idxs in input_indices.items():
            data = data_values.take(input_idxs, axis=axis)

            flat_weights = np.array(overlap_ratios[output_idx])
            reshaped_weights = reshape_weights(flat_weights, data.shape, axis)

            # If data is nan, set weight to nan (so that the point doesn't get used).
            weights = np.where(np.isnan(data), np.nan, reshaped_weights)

            avg = _weighted_average(data, weights, axis=axis)
            output_dataset[var_name][{coord_name: output_idx}] = avg

            if add_metrics:
                std = _weighted_std(data, weights, weighted_avg=avg, axis=axis)
                output_dataset[f"{var_name}_std"][{coord_name: output_idx}] = std

                qc = perform_bin_average_qc_checks(
                    good_mask=good_mask.take(input_idxs, axis=axis),
                    ind_mask=ind_mask.take(input_idxs, axis=axis),
                    bad_mask=bad_mask.take(input_idxs, axis=axis),
                    weights=reshaped_weights,
                    std_dev=std,
                    axis=axis,
                )
                output_dataset[f"qc_{var_name}"][{coord_name: output_idx}] = qc

                # goodfrac = good_mask.take(input_idxs, axis=axis).mean(axis=axis)
                goodfrac = _weighted_average(
                    data=(~bad_mask).take(input_idxs, axis=axis),
                    weights=weights,
                    axis=axis,
                )
                output_dataset[f"{var_name}_goodfraction"][{coord_name: output_idx}] = (
                    goodfrac
                )

    return output_dataset
