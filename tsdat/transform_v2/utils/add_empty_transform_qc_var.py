import numpy as np
import xarray as xr


def add_empty_transform_qc_var(dataset: xr.Dataset, input_var_name: str) -> str:
    """
    Add an empty quality control variable to the dataset for a transformed variable.
    The quality control variable is initialized with zeros and has the same shape
    as the input variable.
    Args:
        dataset (xr.Dataset): The dataset to which the quality control variable will be added.
        input_var_name (str): The name of the input variable for which the quality control
                              variable is being created.
    Returns:
        str: The name of the newly created quality control variable.
    """
    qc_var_name = f"qc_{input_var_name}"
    input_long_name = dataset[input_var_name].attrs.get("long_name", input_var_name)
    dataset[qc_var_name] = xr.full_like(
        dataset[input_var_name], fill_value=0, dtype=np.int64
    )

    dataset[qc_var_name].attrs = dict(
        long_name=f"Quality check results on field: {input_long_name}",
        units="1",
        flag_masks=np.array(
            [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096], dtype=np.uint32
        ),
        flag_meanings=[
            "QC_BAD:  Transformation could not finish, value set to missing_value.",
            "QC_INDETERMINATE:  Some, or all, of the input values used to create this output value had a QC assessment of Indeterminate.",
            "QC_INTERPOLATE:  Indicates a non-standard interpolation using points other than the two that bracket the target index was applied.",
            "QC_EXTRAPOLATE:  Indicates extrapolation is performed out from two points on the same side of the target index.",
            "QC_NOT_USING_CLOSEST:  Nearest good point is not the nearest actual point.",
            "QC_SOME_BAD_INPUTS:  Some, but not all, of the inputs in the averaging window were flagged as bad and excluded from the transform.",
            "QC_ZERO_WEIGHT:  The weights for all the input points to be averaged for this output bin were set to zero.",
            "QC_OUTSIDE_RANGE:  No input samples exist in the transformation region, value set to missing_value.",
            "QC_ALL_BAD_INPUTS:  All the input values in the transformation region are bad, value set to missing_value.",
            "QC_BAD_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_bad_max.",
            "QC_INDETERMINATE_STD:  Standard deviation over averaging interval is greater than limit set by transform parameter std_ind_max.",
            "QC_BAD_GOODFRAC:  Fraction of good and indeterminate points over averaging interval are less than limit set by transform parameter goodfrac_bad_min.",
            "QC_INDETERMINATE_GOODFRAC:  Fraction of good and indeterminate points over averaging interval is less than limit set by transform parameter goodfrac_ind_min.",
        ],
        flag_assessments=[
            "Bad",
            "Indeterminate",
            "Indeterminate",
            "Indeterminate",
            "Indeterminate",
            "Indeterminate",
            "Indeterminate",
            "Bad",
            "Bad",
            "Bad",
            "Indeterminate",
            "Bad",
            "Indeterminate",
        ],
        flag_comments=[
            "An example that will trip this bit is if all values are bad or outside range.",
            "",
            "An example of why this may occur is if one or both of the nearest points was flagged as bad.  Applies only to interpolate transformation method.",
            "This occurs because the input grid does not span the output grid, or because all the points within range and on one side of the target were flagged as bad.  Applies only to the interpolate transformation method.",
            "Applies only to subsample transformation method.",
            "Applies only to the bin average transformation method.",
            'The output "average" value is set to zero, independent of the value of the input.  Applies only to bin average transformation method.',
            'Nearest good bracketing points are farther away than the "range" transform parameter if transformation is done using the interpolate or subsample method, or "width" if a bin average transform is applied.  Test can also fail if more than half an input bin is extrapolated beyond the first or last point of the input grid.',
            "The transformation could not be completed. Values in the output grid are set to missing_value and the QC_BAD bit is also set.",
            "Applies only to the bin average transformation method.",
            "Applies only to the bin average transformation method.",
            "Applies only to the bin average transformation method.",
            "Applies only to the bin average transformation method.",
        ],
        standard_name="quality_flag",
    )
    return qc_var_name
