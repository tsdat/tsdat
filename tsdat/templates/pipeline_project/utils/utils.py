import matplotlib as mpl
from matplotlib.colorbar import Colorbar
import matplotlib.pyplot as plt
from typing import Any

__all__ = ["format_time_xticks", "add_colorbar"]


def format_time_xticks(
    ax: plt.Axes,
    start: int = 4,
    stop: int = 21,
    step: int = 4,
    date_format: str = "%H-%M",
):
    """----------------------------------------------------------------------------
    Formats the ticks on the x-axis of the provided `plt.Axes` nicely. Requires the
    provided `plt.Axis` to already have a plot attached to it and for the x-axis of
    the plotted data to be a datetime object (numpy / pandas / xarray OK). Sets
    major tick locations by hour according to the provided `start`, `stop`, and
    `step` parameters, and labels ticks according to the provided `date_format`.
    Has nice defaults for a plot spanning a 24-hour period.

    Args:
        ax (plt.Axes): The handle for the axes object on which to format the ticks.
        start (int, optional): Hour in which to start the xticks. Defaults to 4.
        stop (int, optional): Hour in which to stop the xticks. Defaults to 21.
        step (int, optional): The step in between major xticks. Defaults to 4.
        date_format (str, optional): The format to use for xtick labels. Defaults
        to "%H-%M".

    ----------------------------------------------------------------------------"""
    ax.xaxis.set_major_locator(mpl.dates.HourLocator(byhour=range(start, stop, step)))  # type: ignore
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter(date_format))  # type: ignore
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)


def add_colorbar(ax: plt.Axes, plot: Any, label: str = "") -> Colorbar:
    """----------------------------------------------------------------------------
    Adds a colorbar to the provided `plt.Axes` object and sets its label. Returns
    the colorbar handle when done.

    Args:
        ax (plt.Axes): The axes in which to draw the colorbar.
        plot (Any): The plot object to use for determining the range and content of
        the colorbar. This `plot` object is returned by calls to `plt.plot()` and
        via calls to `xarray.DataArray.plot(...)`.
        label (str): The label to use for the colorbar. Defaults to "".

    Returns:
        Colorbar: The colorbar object.

    ----------------------------------------------------------------------------"""
    cb = plt.colorbar(plot, ax=ax, pad=0.01)
    cb.ax.set_ylabel(label, fontsize=12)
    cb.outline.set_linewidth(1)
    cb.ax.tick_params(size=0)
    cb.ax.minorticks_off()
    return cb
