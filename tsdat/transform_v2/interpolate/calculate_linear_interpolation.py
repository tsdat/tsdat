import numpy as np
import xarray as xr

from ..utils.create_bounds import create_bounds_from_labels
from ..utils.create_empty_dataset import empty_dataset_like
from ..utils.get_bound_overlaps import get_bound_overlaps
from ..utils.get_filtered_data import get_filtered_data
from ..utils.get_input_variables_for_transform import get_input_variables_for_transform
from ..utils.to_seconds_vec import to_seconds_vec
from ._interpolate_single_point import interpolate_single_point


# TODO: This method gets close to accurate results, but a fundamental misunderstanding
# on my part means it isn't quite right. The trouble occurs when selecting the two
# closest coordinate points to the target/output coordinate value. Because I only
# iterate over the target dimension, I have no way to account for the fact that the QC
# for multidimensional variables *has multiple dimensions* and thus there are possibly
# different combinations of two input points that should be used for each output point.
# To solve this, I'd need to iterate over the other dimensions and apply the
# interpolation and QC logic to each 1D slice of data. I'm pretty certain this is what
# the ADI code does (for BinAverage, too).
# If I fix this by iterating over each of the other dimensions as well, then this will
# most certainly become extremely slow, and investing in some speedups (like numba)
# would probably be worthwhile.
def interpolate(
    input_dataset: xr.Dataset,
    coord_name: str,
    coord_labels: np.ndarray,
    coord_bounds: np.ndarray,
) -> xr.Dataset:
    """
    Perform a linear interpolation on the input dataset based on the specified coordinate.
    This function creates an empty dataset with the desired coordinate labels and bounds,
    and then performs linear interpolation for each variable in the input dataset.
    Args:
        input_dataset (xr.Dataset): The input xarray Dataset to be transformed.
        coord_name (str): The name of the coordinate variable to use for interpolation.
        coord_labels (np.ndarray): The new coordinate labels to align the dataset with.
        coord_bounds (np.ndarray): The bounds for the new coordinate variable.
    Returns:
        xr.Dataset: The transformed xarray Dataset with interpolated values.
    """

    input_data_variables = get_input_variables_for_transform(input_dataset, coord_name)

    output_dataset = empty_dataset_like(
        input_dataset=input_dataset,
        coord_name=coord_name,
        coord_values=coord_labels,
        coord_bounds=coord_bounds,
        add_transform_qc=True,
        add_metric_vars=False,
    )

    if f"{coord_name}_bounds" in input_dataset:
        input_coord_bounds = input_dataset[f"{coord_name}_bounds"].values
    else:  # Infer the bounds from the coordinate values if needed
        input_coord_bounds = create_bounds_from_labels(
            input_dataset[coord_name].values, alignment="center"
        )

    # Calculate the time values we will interpolate from/onto using the midpoints of the
    # bound variables. For time-like coords we convert to seconds from the start time.
    if np.issubdtype(input_coord_bounds.dtype, np.datetime64):
        start_time = input_coord_bounds[0, 0]
        input_coord_bounds = to_seconds_vec(input_coord_bounds - start_time)
        coord_bounds = to_seconds_vec(coord_bounds - start_time)
        coord_labels = to_seconds_vec(coord_labels - start_time)
    input_coord_midpoints = np.mean(input_coord_bounds, axis=1)
    output_coord_midpoints = np.mean(coord_bounds, axis=1)

    # Calculate two mappings that will be useful in the transform loop:
    # - index_mapping: maps each output index to a list of input indexes that fall
    # within the given output coordinate bound.
    # - bound_distances: maps each output index to a list of distances from the output
    # coordinate to each input coordinate index within the given output coordinate bound
    index_mapping, _, bound_distances = get_bound_overlaps(
        input_coord_bounds, coord_bounds
    )

    # Calculate (up to) the two closest input indexes (along the coordinate axis) for
    # each output index. There can be fewer than two input indexes returned if the
    # output bounds do not significantly overlap with the input bounds.
    shortest_distance_idxs = {
        output_idx: np.array(index_mapping[output_idx])[
            np.argpartition(np.abs(distances), kth=1)[:2]
        ]
        for output_idx, distances in bound_distances.items()
        if len(distances) >= 2
    }

    for var_name, data_array in input_data_variables.items():
        qc_var_name = "qc_" + var_name
        axis = data_array.dims.index(coord_name)

        # TODO: Unfortunately this information is not able to be used in this approach.
        _, ind_mask = get_filtered_data(input_dataset, var_name, "Indeterminate")
        filtered_values, bad_mask = get_filtered_data(input_dataset, var_name, "Bad")

        data_values = data_array.values
        # if filter_bad_qc:
        #     data_values = filtered_values

        # TODO: vectorize this loop
        for (output_idx, input_idxs), (_, distances) in zip(
            index_mapping.items(), bound_distances.items()
        ):
            # If there are not enough data points within range of the output coordinate
            # then we cannot perform the transform and need to set the QC_OUTSIDE_RANGE
            # bit (flag=128). We also set QC_BAD (flag=1) because the transform failed.
            if len(distances) < 2:
                output_dataset[qc_var_name][{coord_name: output_idx}] = 128 + 1
                continue

            # this whole approach actually doesn't work for 2D data
            # valid_input_idxs = []
            # valid_input_distances = []
            # for i in range(len(input_idxs)):
            #     if not bad_mask.take(input_idxs[i], axis=axis):
            #         valid_input_idxs.append(input_idxs[i])
            #         valid_input_distances.append(distances[i])
            # If there are fewer than two close points in range after filtering out bad
            # QC, then we set the QC_ALL_BAD_INPUTS bit (flag=256). We also set QC_BAD
            # (flag=1) because the transform failed.
            # if len(valid_input_distances) < 2:
            #     output_dataset[qc_var_name][{coord_name: output_idx}] = 256 + 1
            #     continue

            # now select which 2 points are closest to the output.
            # closest_pts_in_idxs = np.argpartition(np.abs(valid_input_distances), kth=1)[:2]
            # closest_input_idxs = np.array(valid_input_idxs)[closest_pts_in_idxs]
            # d_left, d_right = np.array(valid_input_distances)[closest_pts_in_idxs]

            # now select which 2 points are closest to the output.
            closest_pts_in_idxs = np.argpartition(np.abs(distances), kth=1)[:2]
            closest_input_idxs = np.array(input_idxs)[closest_pts_in_idxs]
            d_left, d_right = np.array(distances)[closest_pts_in_idxs]

            # Set the QC_INTERPOLATE bit (flag=4) if we are using anything other than
            # the two closest coordinate points to the output index. This happens if we
            # wind up filtering out any points due to QC.
            if (closest_input_idxs != shortest_distance_idxs[output_idx]).all():
                output_dataset[qc_var_name][{coord_name: output_idx}] = 4

            # Set the QC_EXTRAPOLATE bit (flag=8) if both points are to the left or both
            # points are to the right of the target coordinate output. This indicates we
            # are performing extrapolation, as opposed to interpolation.
            if (d_left < 0 and d_right < 0) or (d_left > 0 and d_right > 0):
                output_dataset[qc_var_name][{coord_name: output_idx}] += 8

            # Set the QC_INDETERMINATE bit (flag=2) if either of the points has an
            # indeterminate qc evaluation
            # TODO

            # Perform QC Checks
            # - [] 1      (BAD) QC_BAD:  Transformation could not finish, value set to missing_value.
            # - [] 2      (IND) QC_INDETERMINATE:  Some, or all, of the input values used to create this output value had a QC assessment of Indeterminate.

            # print("closest:")
            # print(closest_pts_in_idxs)
            # print(closest_input_idxs)
            # print(closest_distances)

            result = interpolate_single_point(
                x1=input_coord_midpoints[closest_input_idxs[0]],
                x2=input_coord_midpoints[closest_input_idxs[1]],
                y1=data_values.take(closest_input_idxs[0], axis=axis),
                y2=data_values.take(closest_input_idxs[1], axis=axis),
                target_x=output_coord_midpoints[output_idx],
            )

            output_dataset[var_name][{coord_name: output_idx}] = result

    return output_dataset


# def get_shortest_distances(index_mapping, bound_distances):
#     shortest_distance_idxs = {}
#     for output_idx, distances in bound_distances.items():


#     return shortest_distance_idxs
