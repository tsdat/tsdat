from typing import Literal, cast

import numpy as np
import pandas as pd


# TODO: update docstring. This used to be solely for creating time bounds, but now it
# can do both datetime and numeric bounds.
def create_bounds(
    start: float | np.datetime64 | str,
    stop: float | np.datetime64 | str,
    interval: float | str,
    width: float | str | None = None,
    alignment: Literal["left", "right", "center"] | float = "left",
) -> tuple[np.ndarray, np.ndarray]:
    """
    Creates a 2D array of start and end times between the start and stop bounds.
    Returns both the 1D coordinate labels and the 2D array of coordinate bounds.

    Args:
        start (np.datetime64 | str): The starting time corresponding with the first
            bound (inclusive). Can be provided either as a numpy datetime64 scalar or as
            a yyyy-mm-dd hh:mm:ss string. Note the first value of the returned bounds
            array may be less than this if the bins are not left-aligned (see alignment
            parameter for more details.)
        stop (np.datetime64 | str): The ending point of the bounds (exclusive). The
            bounds array is guaranteed to end sometime before this point. Can be
            provided either as a numpy datetime64 scalar or as a yyyy-mm-dd hh:mm:ss
            string.
        interval (int | str): The gap between the centers of the output bounds in
            seconds. Can be provided as either a number or as a string (e.g., "30min")
        width (int | str | None, optional): The difference between the end of each bound
            and the start of each bound in seconds. Can be provided as either a number
            or a string (e.g., "30min"). This can be set independently of the interval
            to make the bounds overlap (if the width is greater than the interval),
            introduce gaps (if the width is less than the interval - why, idk), or to
            make the bounds align perfectly (by setting width = interval, the default).
        alignment (float, optional): Controls where the bounds are located relative to
            the label for each bin. Can be provided as a literal string ("left",
            "right", or "center"), or a value from 0.0 - 1.0 to specify an alignment
            in-between the two (left=0, right=1, center=0.5).

    Raises:
        ValueError: Raises an error if the stop time is not greater than the start time.

    Returns:
        np.ndarray: The 1-dimensional array of labels for the bounds.
        np.ndarray: The 2-dimensional array of time bounds.
    """
    is_datetime_like = isinstance(start, (str, np.datetime64))

    if is_datetime_like:
        _start = 0.0
        _stop = (pd.to_datetime(stop) - pd.to_datetime(start)).total_seconds()
        _interval = pd.to_timedelta(interval).total_seconds()
        _width = _interval if width is None else pd.to_timedelta(width).total_seconds()
    else:
        # Casting just for the type checker since we know that these will all be numbers
        _start = cast(float, start)
        _stop = cast(float, stop)
        _interval = cast(float, interval)
        _width = cast(float, width)

    labels, bounds = _create_bounds(
        start=_start,
        stop=_stop,
        interval=_interval,
        width=_width,
        alignment=alignment,
    )

    if is_datetime_like:
        start_dt = pd.to_datetime(start)
        labels = (pd.to_timedelta(labels, unit="s") + start_dt).to_numpy(
            dtype="datetime64[ns]"
        )
        bound_starts = (pd.to_timedelta(bounds[:, 0], unit="s") + start_dt).to_numpy()
        bound_stops = (pd.to_timedelta(bounds[:, 1], unit="s") + start_dt).to_numpy()
        bounds = np.column_stack((bound_starts, bound_stops)).astype("datetime64[ns]")

    return labels, bounds


def _create_bounds(
    start: float,
    stop: float,
    interval: float,
    width: float | None = None,
    alignment: Literal["left", "right", "center"] | float = "left",
) -> tuple[np.ndarray, np.ndarray]:
    if start >= stop:
        raise ValueError(
            f"Invalid interval: start bound must be less than stop bound. Got ({start}, {stop})"
        )
    if isinstance(alignment, str):
        alignment = {"left": 0.0, "right": 1.0, "center": 0.5}[alignment]
    if width is None:
        width = interval

    labels = np.arange(start, stop, interval)
    offset = alignment * width
    start_values = labels - offset
    end_values = start_values + width
    bounds = np.column_stack((start_values, end_values))

    # np.arange does not properly handle cases where width is a float, and it does not
    # guarantee that the ending bound is open (though it says the ending bound is closed
    # in the docs). So we have to manually check for this and remove that point.
    if (labels[-1] > stop) or np.isclose(labels[-1], stop):
        bounds = bounds[:-1]
        labels = labels[:-1]

    return labels, bounds


def create_bounds_from_labels(
    labels: np.ndarray,  # Can be datetime or float
    width: float | str | None = None,  # Can be str or float or None
    alignment: Literal["left", "right", "center"] | float = "left",  # Literal or 0 to 1
) -> np.ndarray:
    if isinstance(alignment, str):
        alignment = {"left": 0.0, "right": 1.0, "center": 0.5}[alignment]

    is_datetime_like = np.issubdtype(labels.dtype, np.datetime64)

    _width = width
    _labels = labels

    if is_datetime_like:
        np_timedeltas = labels - labels[0]
        _labels = pd.to_timedelta(np_timedeltas).total_seconds()  # type: ignore
        if width is not None:
            _width = pd.to_timedelta(width).total_seconds()

    _width = cast(float, _width)
    _labels = cast(np.ndarray, _labels)

    _bounds = _create_bounds_from_labels(
        labels=_labels,
        width=_width,
        alignment=alignment,
    )

    if is_datetime_like:
        start_dt = pd.to_datetime(labels[0])
        bound_starts = (pd.to_timedelta(_bounds[:, 0], unit="s") + start_dt).to_numpy()
        bound_stops = (pd.to_timedelta(_bounds[:, 1], unit="s") + start_dt).to_numpy()
        _bounds = np.column_stack((bound_starts, bound_stops)).astype("datetime64[ns]")

    return _bounds


def _create_bounds_from_labels(
    labels: np.ndarray,
    width: np.ndarray | float | str | None = None,
    alignment: Literal["left", "right", "center"] | float = "left",
) -> np.ndarray:
    if isinstance(alignment, str):
        alignment = {"left": 0.0, "right": 1.0, "center": 0.5}[alignment]

    # If width is not provided, then we can generate it using the differences between
    # the provided labels. In this case it will be an array, as opposed to a scalar.
    if width is None:
        # With the diff method there will be one less element in the width than the
        # labels. For other uses you would normally prepend '0', but doing so here would
        # result in the first bounds entry having 0 width, which is not what we want. So
        # instead we duplicate the first value and add it back to the front.
        # diff value instead.
        width = np.diff(labels)
        width = np.append(width[0], width)

    start_values = labels - (alignment * width)
    end_values = start_values + width
    bounds = np.column_stack((start_values, end_values))
    return bounds
