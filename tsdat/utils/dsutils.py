from typing import List
import act
from matplotlib import pyplot as plt
import numpy as np
import xarray as xr
from tsdat.constants import ATTS


class DSUtil:
    """-------------------------------------------------------------------
    Provides helper functions for xarray.Dataset
    -------------------------------------------------------------------"""

    @staticmethod
    def get_datastream_name(ds: xr.Dataset):
        pass

    @staticmethod
    def get_end_time(ds: xr.Dataset):
        pass

    @staticmethod
    def get_fail_max(ds: xr.Dataset, variable_name):
        fail_max = None
        fail_range = ds[variable_name].attrs.get(ATTS.FAIL_RANGE, None)
        if fail_range is not None:
            fail_max = fail_range[-1]
        return fail_max

    @staticmethod
    def get_fail_min(ds: xr.Dataset, variable_name):
        fail_min = None
        fail_range = ds[variable_name].attrs.get(ATTS.FAIL_RANGE, None)
        if fail_range is not None:
            fail_min = fail_range[0]
        return fail_min

    @staticmethod
    def get_fill_value(ds: xr.Dataset, variable_name):
        return ds[variable_name].attrs.get(ATTS.FILL_VALUE, None)

    @staticmethod
    def get_non_qc_variables

    @staticmethod
    def get_shape(ds: xr.Dataset, variable_name):
        """-------------------------------------------------------------------
        Convenience method to provide access to dimension names and their
        lengths in one call.
        -------------------------------------------------------------------"""
        var = ds.get(variable_name)
        dims = []
        lengths = []

        for dim in var.sizes:
            dims.append(dim)
            lengths.append(var.sizes[dim])

        return dims, lengths

    @staticmethod
    def get_start_time(ds: xr.Dataset):
        pass

    @staticmethod
    def get_timestamp(dt64: np.datetime64):
        """-------------------------------------------------------------------
        Convert a datetime64 value into a long integer timestamp
        :param dt64: datetime64 object
        :return: timestamp in seconds since 1970-01-01T00:00:00Z
        :rtype: int
        -------------------------------------------------------------------"""
        ts = int((dt64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))
        return ts

    @staticmethod
    def get_variables_with_dimension(ds: xr.Dataset, dim_name, include_qc=False):
        """-------------------------------------------------------------------
        Find all variables dimensioned by the given dim.
        Note that this method will only get data variables, NOT coordinate
        variables.
        -------------------------------------------------------------------"""
        variable_names: List[str] = []
        for variable_name in ds.data_vars:
            if include_qc or not variable_name.startswith('qc_'):
                variable = ds.get(variable_name)
                for dim in variable.sizes:
                    if dim == dim_name:
                        variable_names.append(variable_name)

        return variable_names

    @staticmethod
    def get_warn_max(ds: xr.Dataset, variable_name):
        warn_max = None
        warn_range = ds[variable_name].attrs.get(ATTS.WARN_RANGE, None)
        if warn_range is not None:
            warn_max = warn_range[-1]
        return warn_max

    @staticmethod
    def get_warn_min(ds: xr.Dataset, variable_name):
        warn_min = None
        warn_range = ds[variable_name].attrs.get(ATTS.WARN_RANGE, None)
        if warn_range is not None:
            warn_min = warn_range[0]
        return warn_min

    @staticmethod
    def is_coord_var(ds: xr.Dataset, variable_name):
        """-------------------------------------------------------------------
        :return: True if the given variable is the coordinate variable of a dimension
        :rtype: bool
        -------------------------------------------------------------------"""
        for dim in ds.coords.dims.keys():
            if variable_name == dim:
                return True

        return False

    @staticmethod
    def plot_qc(ds: xr.Dataset, variable_name: str, base_filename: str=None):
        """
        Create a QC plot for the given variable.  This is based on the ACT library:
        https://arm-doe.github.io/ACT/source/auto_examples/plot_qc.html#sphx-glr-source-auto-examples-plot-qc-py

        We provide a convenience wrapper method for basic QC plots of a variable, but
        we recommend to use ACT directly and look at their examples for more complex plots
        like plotting variables in two different datasets.

        TODO: Depending on use cases, we will likely add more arguments to be able to quickly produce
        the most common types of QC plots.
        :param variable_name: The variable to plot
        :param base_filename: The base filename for the image.  Base filename will be prepended
        to .{variable_name}.png
        :return:
        :rtype:
        """

        display = act.plotting.TimeSeriesDisplay(ds, figsize=(15, 10), subplot_shape=(2,))

        # Plot temperature data in top plot
        display.plot(variable_name, subplot_index=(0,))

        # Plot QC data
        display.qc_flag_block_plot(variable_name, subplot_index=(1,))

        # Either display or save the plot, depending upon the parameters passed
        if base_filename:
            filename = f"{base_filename}.{variable_name}.png"
            plt.savefig(filename)
        else:
            plt.show()


#TODO: Maybe we need a method to be able to quickly dump out a summary of the list of problems with the data.
