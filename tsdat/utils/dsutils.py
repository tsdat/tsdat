import mimetypes

import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import xarray as xr
from typing import List, Dict, Tuple, Union, Optional

import act
from tsdat.constants import ATTS

# Note that we can't use these in the type hints because
# importing them here causes a circular dependency
# from tsdat.config import Config, VariableDefinition


mimetypes.init()


class DSUtil:  
    """
    Provides helper functions for xarray.Dataset
    """

    @staticmethod
    def record_corrections_applied(ds: xr.Dataset, variable: str, correction: str):
        """Records a description of a correction made to a variable to the 
        corrections_applied corresponding attribute.

        :param ds: Dataset containing the corrected variable
        :type ds: xr.Dataset
        :param variable: The name of the variable that was corrected
        :type variable: str
        :param correction: A description of the correction
        :type correction: str
        """        
        corrections = ds[variable].attrs.get(ATTS.CORRECTIONS_APPLIED, [])
        corrections.append(correction)
        ds[variable].attrs[ATTS.CORRECTIONS_APPLIED] = corrections

    @staticmethod
    def datetime64_to_string(datetime64: Union[np.ndarray, np.datetime64]) -> Tuple[str, str]:
        """Convert a datetime64 object to formated string.

        :param datetime64: The datetime64 object
        :type datetime64: Union[np.ndarray, np.datetime64]
        :return: A tuple of strings representing the formatted date.  The first string is
            the day in 'yyyymmdd' format.  The second string is the time in 'hhmmss' format.
        :rtype: Tuple[str, str]
        """        
        datetime = act.utils.datetime64_to_datetime()[0]
        return datetime.strftime("%Y%m%d"), datetime.strftime("%H%M%S")

    @staticmethod
    def datetime64_to_timestamp(variable_data: np.ndarray) -> np.ndarray:
        """Converts each datetime64 value to a timestamp in same units as
        the variable (eg., seconds, nanoseconds).

        :param variable_data: ndarray of variable data
        :type variable_data: np.ndarray
        :return: An ndarray of the same shape, with time values converted to 
            long timestamps (e.g., int64)
        :rtype: np.ndarray
        """        
        return variable_data.astype(pd.Timestamp).astype(np.int64)

    @staticmethod
    def get_datastream_name(ds: xr.Dataset = None, config=None) -> str:
        """Returns the datastream name defined in the dataset or in the provided
        pipeline configuration.

        :param ds: The data as an xarray dataset; defaults to None
        :type ds: xr.Dataset, optional.
        :param config: The Config object used to assist reading time data from 
            the raw_dataset; defaults to None.
        :type config: Config, optional
        :return: The datastream name
        :rtype: str
        """        
        assert(ds is not None or config is not None)
        if ds is not None and "datastream_name" in ds.attrs:
            return ds.attrs["datastream_name"]
        elif config :
            return config.dataset_definition.datastream_name
        return None

    @staticmethod
    def get_end_time(ds: xr.Dataset) -> Tuple[str, str]:
        """Convenience method to get the end date and time from a xarray
        dataset.

        :param ds: The dataset
        :type ds: xr.Dataset
        :return: A tuple of [day, time] as formatted strings representing
            the last time point in the dataset.
        :rtype: Tuple[str, str]
        """
        time64 = np.min(ds['time'].data)
        return DSUtil.datetime64_to_string(time64)

    @staticmethod
    def get_fail_max(ds: xr.Dataset, variable_name: str):     
        """Get the max value from the fail_range attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The max value of the fail_range attribute or 
            None if not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """        
        fail_max = None
        fail_range = ds[variable_name].attrs.get(ATTS.FAIL_RANGE, None)
        if fail_range is not None:
            fail_max = fail_range[-1]
        return fail_max

    @staticmethod
    def get_fail_min(ds: xr.Dataset, variable_name):
        """Get the min value from the fail_range attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The min value of the fail_range attribute or 
            None if not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """     
        fail_min = None
        fail_range = ds[variable_name].attrs.get(ATTS.FAIL_RANGE, None)
        if fail_range is not None:
            fail_min = fail_range[0]
        return fail_min

    @staticmethod
    def get_valid_max(ds: xr.Dataset, variable_name):
        """Get the max value from the valid_range attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The max value of the valid_range attribute or 
            None if not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """     
        valid_max = None
        valid_range = ds[variable_name].attrs.get(ATTS.VALID_RANGE, None)
        if valid_range is not None:
            valid_max = valid_range[-1]
        return valid_max

    @staticmethod
    def get_valid_min(ds: xr.Dataset, variable_name):
        """Get the min value from the valid_range attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The min value of the valid_range attribute or 
            None if not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """     
        valid_min = None
        valid_range = ds[variable_name].attrs.get(ATTS.VALID_RANGE, None)
        if valid_range is not None:
            valid_min = valid_range[0]
        return valid_min

    @staticmethod
    def get_fill_value(ds: xr.Dataset, variable_name: str):
        """Get the value of the _FillValue attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The value of the _FillValue attr or None
            if it is not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """        
        return ds[variable_name].attrs.get(ATTS.FILL_VALUE, None)

    @staticmethod
    def get_non_qc_variable_names(ds: xr.Dataset) -> List[str]:
        """Get a list of all data variables in the dataset that
        are NOT qc variables.

        :param ds: A dataset
        :type ds: xr.Dataset
        :return: List of non-qc data variable names
        :rtype: List[str]
        """        
        varnames = []

        def exclude_qc(variable_name):
            if variable_name.startswith('qc_'):
                return False
            else:
                return True

        varnames = filter(exclude_qc, list(ds.data_vars.keys()))

        return varnames

    @staticmethod
    def get_raw_end_time(raw_ds: xr.Dataset, time_var_definition) -> Tuple[str, str]:
        """Convenience method to get the end date and time from a raw xarray
        dataset. This uses `time_var_definition.get_input_name()` as the
        dataset key for the time variable and additionally uses the input's
        `Converter` object if applicable.

        :param raw_ds: A raw dataset (not standardized)
        :type raw_ds: xr.Dataset
        :param time_var_definition: The 'time' variable definition from the 
            pipeline config
        :type time_var_definition: VariableDefinition
        :return: A tuple of strings representing the last time data point
            in the dataset.  The first string is the day in 'yyyymmdd' format.  
            The second string is the time in 'hhmmss' format.
        :rtype: Tuple[str, str]
        """        
        time_var_name = time_var_definition.get_input_name()
        time_data = raw_ds[time_var_name].values

        time64_data = time_var_definition.run_converter(time_data)

        end_datetime64 = np.max(time64_data)
        end: datetime.datetime = act.utils.datetime64_to_datetime(end_datetime64)[0]
        return end.strftime("%Y%m%d"), end.strftime("%H%M%S")

    @staticmethod
    def get_raw_start_time(raw_ds: xr.Dataset, time_var_definition) -> Tuple[str, str]:
        """Convenience method to get the start date and time from a raw xarray
        dataset. This uses `time_var_definition.get_input_name()` as the
        dataset key for the time variable and additionally uses the input's
        `Converter` object if applicable.

        :param raw_ds: A raw dataset (not standardized)
        :type raw_ds: xr.Dataset
        :param time_var_definition: The 'time' variable definition from the 
            pipeline config
        :type time_var_definition: VariableDefinition
        :return: A tuple of strings representing the first time data point
            in the dataset.  The first string is the day in 'yyyymmdd' format.  
            The second string is the time in 'hhmmss' format.
        :rtype: Tuple[str, str]
        """        
        time_var_name = time_var_definition.get_input_name()
        time_data = raw_ds[time_var_name].values

        time64_data = time_var_definition.run_converter(time_data)

        start_datetime64 = np.min(time64_data)
        start: datetime.datetime = act.utils.datetime64_to_datetime(start_datetime64)[0]
        return start.strftime("%Y%m%d"), start.strftime("%H%M%S")

    @staticmethod
    def get_coordinate_variable_names(ds: xr.Dataset) -> List[str]:
        """Get a list of all coordinate variables in this dataset.

        :param ds: The dataset
        :type ds: xr.Dataset
        :return: List of coordinate variable names
        :rtype: List[str]
        """        
        return list(ds.coords.keys())

    @staticmethod
    def get_shape(ds: xr.Dataset, variable_name: str) -> Tuple[List[str], List[int]]:
        """Get the shape of a variable's data.  Convenience 
        method to provide access to dimension names and their
        lengths in one call.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: A tuple where the first value is an array of the dimension
            names belonging to this variable and the second value is an array
            of the corresponding lengths of those dimensions.
        :rtype: Tuple[List[str], List[int]]
        """
        var = ds.get(variable_name)
        dims = []
        lengths = []

        for dim in var.sizes:
            dims.append(dim)
            lengths.append(var.sizes[dim])

        return dims, lengths

    @staticmethod
    def get_start_time(ds: xr.Dataset) -> Tuple[str, str]:
        """Convenience method to get the start date and time from a xarray
        dataset.

        :param ds: A standardized dataset
        :type ds: xr.Dataset
        :return: A tuple of strings representing the first time data point
            in the dataset.  The first string is the day in 'yyyymmdd' format.  
            The second string is the time in 'hhmmss' format.
        :rtype: Tuple[str, str]
        """        
        time64 = np.min(ds['time'].data)
        start = act.utils.datetime64_to_datetime(time64)[0]
        return start.strftime("%Y%m%d"), start.strftime("%H%M%S")

    @staticmethod
    def get_timestamp(dt64: np.datetime64):
        """Convert a datetime64 value into a long integer timestamp
        :param dt64: datetime64 object
        :return: timestamp in seconds since 1970-01-01T00:00:00Z
        :rtype: int
        """
        ts = int((dt64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))
        return ts

    @staticmethod
    def get_variables_with_dimension(ds: xr.Dataset, dim_name: str, include_qc=False) -> List[str]:
        """Find all variables dimensioned by the given dim.
        Note that this method will only get data variables, NOT coordinate
        variables.

        :param ds: A dataset
        :type ds: xr.Dataset
        :param dim_name: Dimension name
        :type dim_name: str
        :param include_qc: Should qc variables be included, defaults to False
        :type include_qc: bool, optional
        :return: A list of all variable names that have that dimension
        :rtype: List[str]
        """        
        variable_names: List[str] = []
        for variable_name in ds.data_vars:
            if include_qc or not variable_name.startswith('qc_'):
                variable = ds.get(variable_name)
                for dim in variable.sizes:
                    if dim == dim_name:
                        variable_names.append(variable_name)
        return variable_names

    @staticmethod
    def get_metadata(ds: xr.Dataset) -> Dict:
        """Get a dictionary of all global and variable
        attributes in a dataset.  Global atts are found
        under the 'attributes' key and variable atts are
        found under the 'variables' key.

        :param ds: A dataset
        :type ds: xr.Dataset
        :return: A dictionary of global & variable attributes
        :rtype: Dict
        """        
        attributes = ds.attrs
        variables = {var_name: ds[var_name].attrs for var_name in ds.variables}
        metadata = {"attributes": attributes, "variables": variables}
        return metadata

    @staticmethod
    def get_warn_max(ds: xr.Dataset, variable_name):
        """Get the max value from the warn_range attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The max value of the warn_range attribute or 
            None if not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """   
        warn_max = None
        warn_range = ds[variable_name].attrs.get(ATTS.WARN_RANGE, None)
        if warn_range is not None:
            warn_max = warn_range[-1]
        return warn_max

    @staticmethod
    def get_warn_min(ds: xr.Dataset, variable_name):
        """Get the min value from the warn_range attribute
        for the given variable.

        :param ds: The dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: The min value of the warn_range attribute or 
            None if not defined
        :rtype: same data type of the variable (int, float, etc.)
            or None
        """   
        warn_min = None
        warn_range = ds[variable_name].attrs.get(ATTS.WARN_RANGE, None)
        if warn_range is not None:
            warn_min = warn_range[0]
        return warn_min

    @staticmethod
    def is_coord_var(ds: xr.Dataset, variable_name: str) -> bool:
        """Determine if given variable is a coordinate variable
        for a dimension.

        :param ds: A dataset
        :type ds: xr.Dataset
        :param variable_name: A variable in the dataset
        :type variable_name: str
        :return: True if the variable is a coordinate variable
        :rtype: bool
        """        
        for dim in ds.coords.dims.keys():
            if variable_name == dim:
                return True

        return False

    @staticmethod
    def plot_qc(ds: xr.Dataset, variable_name: str, filename: str=None):       
        """Create a QC plot for the given variable.  This is based on the ACT library:
        https://arm-doe.github.io/ACT/source/auto_examples/plot_qc.html#sphx-glr-source-auto-examples-plot-qc-py

        We provide a convenience wrapper method for basic QC plots of a variable, but
        we recommend to use ACT directly and look at their examples for more complex plots
        like plotting variables in two different datasets.

        TODO: Depending on use cases, we will likely add more arguments to be able to quickly produce
        the most common types of QC plots.
        
        :param ds: A dataset
        :type ds: xr.Dataset
        :param variable_name: The variable to plot
        :type variable_name: str
        :param filename: The filename for the image.  Saves the plot as this filename if provided.
        :type filename: str, optional
        """

        display = act.plotting.TimeSeriesDisplay(ds, figsize=(15, 10), subplot_shape=(2,))

        # Plot temperature data in top plot
        display.plot(variable_name, subplot_index=(0,))

        # Plot QC data
        display.qc_flag_block_plot(variable_name, subplot_index=(1,))

        # Either display or save the plot, depending upon the parameters passed
        if filename:
            plt.savefig(filename)
        else:
            plt.show()

    @staticmethod
    def get_plot_filename(dataset: xr.Dataset, plot_description: str, extension: str) -> str:
        """Returns the filename for a plot according to MHKIT-Cloud Data
        standards. The dataset is used to determine the datastream_name and
        start date/time. The standards dictate that a plot filename should
        follow the format: `datastream_name.date.time.description.extension`.

        :param dataset: The dataset from which the plot data is drawn from.
            This is used to collect the datastream_name and start date/time.
        :type dataset: xr.Dataset
        :param plot_description: The description of the plot. Should be as 
            brief as possible and contain no spaces. Underscores may be used.
        :type plot_description: str
        :param extension: The file extension for the plot.
        :type extension: str
        :return: The standardized plot filename.
        :rtype: str
        """        
        datastream_name = DSUtil.get_datastream_name(dataset)
        date, time = DSUtil.get_start_time(dataset)
        return f"{datastream_name}.{date}.{time}.{plot_description}.{extension}"

    @staticmethod
    def get_dataset_filename(dataset: xr.Dataset, file_extension=".nc") -> str:
        """Given an xarray dataset this function will return the base filename of
        the dataset according to MHkiT-Cloud data standards. The base filename
        does not include the directory structure where the file should be
        saved, only the name of the file itself, e.g.
        z05.ExampleBuoyDatastream.b1.20201230.000000.nc

        :param dataset: The dataset whose filename should be generated.
        :type dataset: xr.Dataset
        :param file_extension: The file extension to use. Defaults to ".nc"
        :type file_extension: str, optional
        :return: The base filename of the dataset.
        :rtype: str
        """        
        datastream_name = DSUtil.get_datastream_name(dataset)
        start_date, start_time = DSUtil.get_start_time(dataset)
        return f"{datastream_name}.{start_date}.{start_time}{file_extension}"

    @staticmethod
    def get_raw_filename(raw_dataset: xr.Dataset, old_filename: str, config) -> str:
        """Returns the appropriate raw filename of the raw dataset according to
        MHKIT-Cloud naming conventions. Uses the config object to parse the
        start date and time from the raw dataset for use in the new filename.

        The new filename will follow the MHKIT-Cloud Data standards for raw
        filenames, ie: `datastream_name.date.time.raw.old_filename`, where the
        data level used in the datastream_name is `00`.

        :param raw_dataset: The raw data as an xarray dataset.
        :type raw_dataset: xr.Dataset
        :param old_filename: The name of the original raw file.
        :type old_filename: str
        :param config: The Config object used to assist reading time data from 
            the raw_dataset.
        :type config: Config
        :return: The standardized filename of the raw file.
        :rtype: str
        """        
        original_filename = os.path.basename(old_filename)
        raw_datastream_name = config.pipeline_definition.input_datastream_name
        time_var = config.dataset_definition.get_variable('time')
        start_date, start_time = DSUtil.get_raw_start_time(raw_dataset, time_var)
        return f"{raw_datastream_name}.{start_date}.{start_time}.raw.{original_filename}"

    @staticmethod
    def get_date_from_filename(filename: str) -> str:
        """Given a filename that conforms to MHKiT-Cloud Data Standards, return
        the date of the first point of data in the file.

        :param filename: The filename or path to the file.
        :type filename: str
        :return: The date, in "yyyymmdd.hhmmss" format.
        :rtype: str
        """     
        filename = os.path.basename(filename)
        date = filename.split(".")[3]
        time = filename.split(".")[4]
        return f"{date}.{time}"

    @staticmethod
    def get_datastream_name_from_filename(filename: str) -> Optional[str]:
        """Given a filename that conforms to MHKiT-Cloud Data Standards, return
        the datastream name.  Datastream name is everything to the left of the
        third '.' in the filename.
        
        e.g., humboldt_ca.buoy_data.b1.20210120.000000.nc

        :param filename: The filename or path to the file.
        :type filename: str
        :return: The datstream name, or None if filename is not in proper format.
        :rtype: Optional[str]
        """        
        datastream_name = None

        parts = filename.split(".")
        if len(parts) > 2:
            datastream_name = f'{parts[0]}.{parts[1]}.{parts[2]}'

        return datastream_name

    @staticmethod
    def get_datastream_directory(datastream_name: str, root: str = "") -> str:
        """Given the datastream_name and an optional root, returns the path to
        where the datastream should be located. Does NOT create the directory
        where the datastream should be located.

        :param datastream_name: The name of the datastream whose directory path should
            be generated.
        :type datastream_name: str
        :param root: The directory to use as the root of the directory structure.
            Defaults to None. Defaults to ""
        :type root: str, optional
        :return: The path to the directory where the datastream should be located.
        :rtype: str
        """        
        location_id = datastream_name.split(".")[0]
        return os.path.join(root, location_id, datastream_name)

    @staticmethod
    def is_image(filename: str) -> bool:
        """Detect the mimetype from the file extension and use it to determine
        if the file is an image or not

        :param filename: The name of the file to check
        :type filename: str
        :return: True if the file extension matches an image mimetype
        :rtype: bool
        """
        is_an_image = False
        mimetype = mimetypes.guess_type(filename)[0]
        if mimetype is not None:
            mimetype = mimetype.split('/')[0]
            if mimetype == 'image':
                is_an_image = True

        return is_an_image

# TODO: Maybe we need a method to be able to quickly dump out a summary of the list of problems with the data.


