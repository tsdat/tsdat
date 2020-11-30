from typing import List, Any
import act
from matplotlib import pyplot as plt
import numpy as np
import xarray
from tsdat import Config


class Atts():
    # Standard attributes
    DATA_TYPE = 'data_type'
    UNITS = 'units'
    DESCRIPTION = 'description'
    VALID_DELTA = 'valid_delta'
    VALID_MIN = 'valid_min'
    VALID_MAX = 'valid_max'
    MISSING_VALUE = 'missing_value'


class TimeSeriesDataset:
    ATTS = Atts()

    """
    Wrapper for xarray.Dataset that provides helper QC and utility functions
    """

    @staticmethod
    def get_timestamp(dt64: np.datetime64):
        """
        Convert a datetime64 value into a long integer timestamp
        :param dt64: datetime64 object
        :return: timestamp in seconds since 1970-01-01T00:00:00Z
        :rtype: int
        """
        ts = int((dt64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))
        return ts

    def __init__(self, dataset: xarray.Dataset, config: Config = None):

        # First check if the dataset has a time dimension and time coordinate variable - if not, then fail
        time_var = dataset.get('time', None)
        if time_var is None:
            raise ValueError("TimeSeriesDataset requires a time variable!")

        time_dim = dataset.coords.dims.get('time', None)
        if time_dim is None:
            raise ValueError("TimeSeriesDataset requires a time dimension!")

        # If config is passed, then make sure all the variable attributes specified in the config
        # are added to the dataset's variable attributes
        if config:
            for variable in config.get_variables():
                ds_var = dataset.get(variable.name)
                for att_name in variable.attrs:
                    ds_var.attrs[att_name] = variable.attrs[att_name]

        # Make sure the xarray data is in the correct format.
        # By calling decode_cf, xarray will make sure the time values are
        # converted to numpy datetime64 units which it needs.
        # ---->Also, it moves the units and missing_value to the encoding, not the atrrs!
        # TODO: this correctly converts everything, but it messes up the dataset - after
        # I run this, I can no longer set any data values!  Need to understand this better...
        #dataset = xarray.decode_cf(dataset)
        self.xr: xarray.Dataset = dataset

    def set_datastream_name(self, ds_name: str, override=True):
        existing_name = self.xr.attrs.get('datastream', None)
        if override or existing_name is None:
            self.xr.attrs['datastream'] = ds_name

    def close(self):
        """
        Close the underlying xarray dataset
        """
        self.xr.close()

    def get_var(self, variable_name: str):
        return self.xr.get(variable_name)

    def get_shape(self, variable_name):
        """
        Since Xarray does not provide access to dimension names and
        lengths via a simple array that I can iterate over via an
        index position, I added this wrapper to get my own.
        """
        var = self.xr.get(variable_name)
        dims = []
        lengths = []

        for dim in var.sizes:
            dims.append(dim)
            lengths.append(var.sizes[dim])

        return dims, lengths

    def get_or_create_qc_var(self, variable_name: str):
        """
        Get the companion qc variable for the given variable.  If the
        qc var does not exist, it will be created.
        :return:
        :rtype: xarray.DataArray
        """
        qc_var_name = f"qc_{variable_name}"
        qc_var = self.xr.get(qc_var_name)
        if qc_var is None:
            qc_var = xarray.zeros_like(self.xr[variable_name], np.int32)
            qc_var.attrs.clear()  # clear old values
            qc_var.name = qc_var_name # rename it
            qc_var.attrs['data_type'] = 'int'
            # add standard_name attr so it will be compatible with act plotting tools & ARM recommendations
            qc_var.attrs['standard_name'] = 'quality_flag'
            self.xr[qc_var_name] = qc_var

            # add ancillary_variables to var so it will be compatible with act plotting tools
            var = self.xr.get(variable_name)
            var.attrs['ancillary_variables'] = qc_var_name
            qc_var = self.xr.get(qc_var_name)

        return qc_var

    def is_coord_var(self, variable_name):
        """
        :return: True if the given variable is the coordinate variable of a dimension
        :rtype: bool
        """
        for dim in self.xr.coords.dims.keys():
            if variable_name == dim:
                return True

        return False

    def get_variables_with_dimension(self, dim_name, include_qc=False):
        """
        Note that this method will only get data variables,
        NOT coordinate variables.
        """
        variable_names: List[str] = []
        for variable_name in self.xr.data_vars:
            if include_qc or not variable_name.startswith('qc_'):
                variable = self.xr.get(variable_name)
                for dim in variable.sizes:
                    if dim == dim_name:
                        variable_names.append(variable_name)

        return variable_names

    def get_value_at_flattened_position(self, var_name: str, position: int):
        """
        Fetch a value from a variable at a given position.  If the variable is
        multidimensional, the position will be flattened to a one-dimensional index.
        For example, if the variable's data shape looks like:
        [1, 2.5]
        [2, 4.5]
        [3, 8.5]

        Then the position will be accessed as if the array were this
        [1, 2.5, 2, 4.5, 3, 8.5]
        So the value at position 4 would be 3.
        :return:
        :rtype:
        """
        var_iter = np.nditer(self.xr.get(var_name).values)

        with var_iter:
            while not var_iter.finished:
                if var_iter.iterindex == position:
                    return var_iter[0].item()
                var_iter.iternext()

        return None

    def get_missing_value(self, variable_name: str):
        # Default to -9999
        ds_var = self.xr.get(variable_name)

        missing_value = ds_var.attrs.get(TimeSeriesDataset.ATTS.MISSING_VALUE, -9999)
        return missing_value

    def is_missing(self, variable_name: str, value: Any):
        """
        Check if this value is a missing value for the given variable
        TODO: xarray will save missing values as nan, so we should check
        for nan as well as specific missing value.  Also, comparing
        against missing_value attr won't work for datetime64 values.
        See if xarray or ACT has a method to do this check properly.
        :rtype: bool
        """
        missing_value = self.get_missing_value(variable_name)
        is_missing_value = (value == missing_value)

        return is_missing_value

    def get_previous_value(self, variable_name:str, coordinates: List[int], axis: int):
        """
        Get the previous value of the given variable along the given axis
        :param variable_name:
        :param coordinates: Array of coordinates, one for each dimension of this variable.
        len(coordinates) is the number of dimensions.
        coordinates[0] is usually the position for the time dimension
        :param axis: Specify 0, 1, 2 to indicate which dimension to navigate on.
        If not specified, defaults to the last dimension.
        :return: previous value
        :rtype: Any or None
        """
        if not axis:
            axis = len(coordinates) - 1

        if axis > len(coordinates) - 1:
            raise ValueError("Axis is larger than the number of dimensions")

        previous_value = None
        if coordinates[axis] > 0:
            new_coords = coordinates.copy()
            new_coords[axis] = coordinates[axis] - 1
            var = self.xr.get(variable_name)

            if len(new_coords) == 1:
                x = new_coords[0]
                previous_value = var.values[x]

            elif len(new_coords) == 2:
                x = new_coords[0]
                y = new_coords[1]
                previous_value = var.values[x][y]

            elif len(new_coords) == 3:
                x = new_coords[0]
                y = new_coords[1]
                z = new_coords[2]
                previous_value = var.values[x][y][z]

        return previous_value

    def plot_qc(self, variable_name: str, base_filename: str=None):
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

        display = act.plotting.TimeSeriesDisplay(self.xr, figsize=(15, 10), subplot_shape=(2,))

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
