import numpy as np


def perform_bin_average_qc_checks(
    good_mask: np.ndarray,
    ind_mask: np.ndarray,
    bad_mask: np.ndarray,
    weights: np.ndarray,
    std_dev: np.ndarray,
    axis: int,
    GOODFRAC_IND_MIN: float = 0.15,
    GOODFRAC_BAD_MIN: float = 0.05,
) -> np.ndarray:
    """
    Perform quality control checks on the results of a bin average transformation.
    Args:
        good_mask (np.ndarray): A boolean mask indicating good data points.
        ind_mask (np.ndarray): A boolean mask indicating indeterminate data points.
        bad_mask (np.ndarray): A boolean mask indicating bad data points.
        weights (np.ndarray): The weights used in the bin average transformation.
        std_dev (np.ndarray): The standard deviation of the data points in the bin.
        axis (int): The axis along which the bin average was performed.
        GOODFRAC_IND_MIN (float): Minimum fraction of good and indeterminate points for
            the QC_INDETERMINATE_GOODFRAC check.
        GOODFRAC_BAD_MIN (float): Minimum fraction of good and indeterminate points for
            the QC_BAD_GOODFRAC check.
    Returns:
        np.ndarray: An array of quality control flags for each bin.
    """
    # The QC flags are defined as follows:
    # X 1       "QC_BAD:  Transformation could not finish, value set to missing_value.",
    # X 2       "QC_INDETERMINATE:  Some, or all, of the input values used to create this output value had a QC assessment of Indeterminate.",
    #   4       "QC_INTERPOLATE:  Indicates a non-standard interpolation using points other than the two that bracket the target index was applied.",
    #   8       "QC_EXTRAPOLATE:  Indicates extrapolation is performed out from two points on the same side of the target index.",
    #   16      "QC_NOT_USING_CLOSEST:  Nearest good point is not the nearest actual point.",
    # X 32      "QC_SOME_BAD_INPUTS:  Some, but not all, of the inputs in the averaging window were flagged as bad and excluded from the transform.",
    # X 64      "QC_ZERO_WEIGHT:  The weights for all the input points to be averaged for this output bin were set to zero.",
    # X 128     "QC_OUTSIDE_RANGE:  No input samples exist in the transformation region, value set to missing_value.",
    # X 256     "QC_ALL_BAD_INPUTS:  All the input values in the transformation region are bad, value set to missing_value.",
    # X 512     "QC_BAD_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_bad_max.",
    # X 1024    "QC_INDETERMINATE_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_ind_max.",
    # X 2048    "QC_BAD_GOODFRAC:  Fraction of good and indeterminate points over averaging interval are less than limit set by transform parameter goodfrac_bad_min.",
    # X 4096    "QC_INDETERMINATE_GOODFRAC:  Fraction of good and indeterminate points over averaging interval is less than limit set by transform parameter goodfrac_ind_min.",

    # QC_OUTSIDE_RANGE:  No input samples exist in the transformation region, value set to missing_value.
    if weights.shape[axis] == 0:
        return np.full_like(std_dev, 129, dtype=int)  # also QC_BAD -- bit 1

    bad_fraction = bad_mask.mean(axis=axis)
    good_fraction = (~bad_mask).mean(axis=axis)

    # Start performing checks
    result = np.zeros(std_dev.shape, dtype=int)

    # QC_INDETERMINATE:  Some, or all, of the input values used to create this output value had a QC assessment of Indeterminate
    result |= 2 * (0 < np.sum(ind_mask, axis=axis))

    # QC_SOME_BAD_INPUTS:  Some, but not all, of the inputs in the averaging window were flagged as bad and excluded from the transform.
    result |= 32 * (np.less(0, bad_fraction) & np.less(bad_fraction, 1))

    # QC_ZERO_WEIGHT:  The weights for all the input points to be averaged for this output bin were set to zero.
    result |= 64 * (np.nansum(weights, axis=axis) == 0)

    # QC_ALL_BAD_INPUTS:  All the input values in the transformation region are bad, value set to missing_value.
    result |= 257 * (np.isclose(bad_fraction, 1.0))  # add 1 for QC_BAD

    # QC_BAD_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_bad_max.
    # result |= 512 * (0)  # TODO

    # QC_INDETERMINATE_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_ind_max.
    # result |= 1024 * (0)  # TODO

    # QC_BAD_GOODFRAC:  Fraction of good and indeterminate points over averaging interval are less than limit set by transform parameter goodfrac_bad_min.
    result |= 2048 * (good_fraction < GOODFRAC_BAD_MIN)

    # QC_INDETERMINATE_GOODFRAC:  Fraction of good and indeterminate points over averaging interval is less than limit set by transform parameter goodfrac_ind_min.
    result |= 4096 * (good_fraction < GOODFRAC_IND_MIN)

    return result
