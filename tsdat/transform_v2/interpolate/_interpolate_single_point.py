def interpolate_single_point(
    x1: float, x2: float, y1: float, y2: float, target_x: float
):
    """Interpolate a single point given bounds and input values.

    Parameters:
    - x1_bounds: Tuple representing the bounds of the first interval (e.g., (x1_start, x1_end)).
    - x2_bounds: Tuple representing the bounds of the second interval (e.g., (x2_start, x2_end)).
    - y1: Y value corresponding to the first interval.
    - y2: Y value corresponding to the second interval.
    - target_x: The target X value where interpolation is to be evaluated.

    Returns:
    - Interpolated Y value at the target X value.
    """
    # x1_start, x1_end = x1_bounds
    # x2_start, x2_end = x2_bounds

    # # Determine x1 and x2 based on the provided bounds
    # x1 = (x1_start + x1_end) / 2
    # x2 = (x2_start + x2_end) / 2

    # Perform interpolation if target_x is within the bounds
    m = (y2 - y1) / (x2 - x1)

    if x1 <= target_x <= x2:
        interpolated_y = y1 + m * (target_x - x1)
    # Handle extrapolation if target_x is outside the provided ranges
    elif target_x < x1:
        interpolated_y = y1 + m * (target_x - x1)
    else:  # target_x > x2_end
        interpolated_y = y2 + m * (target_x - x2)

    return interpolated_y
