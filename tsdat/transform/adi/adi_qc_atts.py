adi_qc_atts = {
    "bit_1_description": (
        "QC_BAD:  Transformation could not finish, value set to missing_value."
    ),
    "bit_1_assessment": "Bad",
    "bit_2_description": (
        "QC_INDETERMINATE:  Some, or all, of the input values used to create this"
        " output value had a QC assessment of Indeterminate."
    ),
    "bit_2_assessment": "Indeterminate",
    "bit_3_description": (
        "QC_INTERPOLATE:  Indicates a non-standard interpolation using points other"
        " than the two that bracket the target index was applied."
    ),
    "bit_3_assessment": "Indeterminate",
    "bit_4_description": (
        "QC_EXTRAPOLATE:  Indicates extrapolation is performed out from two points on"
        " the same side of the target index."
    ),
    "bit_4_assessment": "Indeterminate",
    "bit_5_description": (
        "QC_NOT_USING_CLOSEST:  Nearest good point is not the nearest actual point."
    ),
    "bit_5_assessment": "Indeterminate",
    "bit_6_description": (
        "QC_SOME_BAD_INPUTS:  Some, but not all, of the inputs in the averaging window"
        " were flagged as bad and excluded from the transform."
    ),
    "bit_6_assessment": "Indeterminate",
    "bit_7_description": (
        "QC_ZERO_WEIGHT:  The weights for all the input points to be averaged for this"
        " output bin were set to zero."
    ),
    "bit_7_assessment": "Indeterminate",
    "bit_8_description": (
        "QC_OUTSIDE_RANGE:  No input samples exist in the transformation region, value"
        " set to missing_value."
    ),
    "bit_8_assessment": "Bad",
    "bit_9_description": (
        "QC_ALL_BAD_INPUTS:  All the input values in the transformation region are bad,"
        " value set to missing_value."
    ),
    "bit_9_assessment": "Bad",
    "bit_10_description": (
        "QC_BAD_STD:  Standard deviation over averaging interval is greater than limit"
        " set by transform parameter std_bad_max."
    ),
    "bit_10_assessment": "Bad",
    "bit_11_description": (
        "QC_INDETERMINATE_STD:  Standard deviation over averaging interval is greater"
        " than limit set by transform parameter std_ind_max."
    ),
    "bit_11_assessment": "Indeterminate",
    "bit_12_description": (
        "QC_BAD_GOODFRAC:  Fraction of good and indeterminate points over averaging"
        " interval are less than limit set by transform parameter goodfrac_bad_min."
    ),
    "bit_12_assessment": "Bad",
    "bit_13_description": (
        "QC_INDETERMINATE_GOODFRAC:  Fraction of good and indeterminate points over"
        " averaging interval is less than limit set by transform parameter"
        " goodfrac_ind_min."
    ),
    "bit_13_assessment": "Indeterminate",
}
