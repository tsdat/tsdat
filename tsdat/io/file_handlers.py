import os
import abc
import yaml
import functools
import numpy as np
import pandas as pd
import xarray as xr
from typing import List, Dict
from tsdat.config import Config
from tsdat.utils import DSUtil

FILEHANDLERS = dict()


def register_filehandler(file_extension: str):
    """-----------------------------------------------------------------------
    Python decorator to register a class in the FILEHANDLERS dictionary. This
    dictionary will be used by the MHKiT-Cloud pipeline to read and write raw,
    intermediate, and processed data, as well as in the DatastreamStorage 
    class to store the final output dataset.

    Example Usage:
    ```
    @register_filehandler([".nc", ".cdf"])
    class NetCdfHandler(FileHandler):
        def write(self, dataset, filename):
            pass
        def read(self, filename, config):
            pass
    ```

    Args:
        file_extension (str):   The file extension that the FileHandler should
                                be used to read from and write to files ending
                                in this extension. This can also be provided 
                                as a list if multiple file extensions are used
                                for the same type of file.        
    -----------------------------------------------------------------------"""
    def decorator_register(func):
        if isinstance(file_extension, List):
            for ext in file_extension:
                FILEHANDLERS[ext] = func
        else:
            FILEHANDLERS[file_extension] = func
        @functools.wraps(func)
        def wrapper_register(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper_register
    return decorator_register


class FileHandler(abc.ABC):

    @abc.abstractmethod
    def write(self, ds: xr.Dataset, filename: str, **kwargs) -> None:
        """
        Classes derived from the FileHandler class must implement this method.
        ----------------------------------------------------------------------
        Saves the given dataset to file.

        Args:
            ds (xr.Dataset): The dataset to save.
            filename (str): An absolute or relative path to the file including
                            filename.
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def read(self, filename: str, config: Config = None, **kwargs) -> xr.Dataset:
        """
        Classes derived from the FileHandler class must implement this method.
        ----------------------------------------------------------------------
        This method reads the given file into a xr.Dataset object.

        Args:
            filename (str): The path to the file to read in.
            config (Config, optional):  Optional Config object. Defaults to 
                                        None.

        Returns:
            xr.Dataset: A xr.Dataset object
        -------------------------------------------------------------------"""
        pass


@register_filehandler([".nc", ".cdf"])
class NetCdfHandler(FileHandler):

    def write(self, ds: xr.Dataset, filename: str, **kwargs):
        ds.to_netcdf(filename, format='NETCDF4')

    def read(self, filename: str, config: Config = None):
        # TODO: need to have xr.Dataset close the file automatically if user
        #  uses "with" - add a resource manager api
        ds_disk = xr.open_dataset(filename)
        # TODO: Use config?
        return xr.Dataset(ds_disk)


# @register_filehandler(".csv")
class CsvHandler(FileHandler):

    def write(self, ds: xr.Dataset, filename: str, **kwargs):
        # You can only write one-dimensional data to csv
        if len(ds.dims) > 1:
            raise TypeError("Dataset has more than one dimension, so it can't be saved to csv.  Try netcdf instead.")

        # First convert the data to a Pandas DataFrame and
        # save the variable metadata in a dictionary
        variables = {}
        df: pd.DataFrame = pd.DataFrame()
        for variable_name in ds.variables:
            variable = ds[variable_name]
            df[variable_name] = variable.to_pandas()
            variables[variable_name] = self.variable_to_dict(ds, variable_name)

        # Then save the DataFrame to a csv file
        kwargs['index'] = False
        df.to_csv(filename, **kwargs)

        # Now save all the metadata dictionary to a companion yaml file
        metadata = { "variables": variables }
        yaml_filename = f"{filename}.yaml"
        with open(yaml_filename, 'w') as file:
            yaml.dump(metadata, file)

    def read(self, filename: str, config: Config = None, **kwargs):
        # First read the csv into a pandas dataframe
        dataframe: pd.DataFrame = pd.read_csv(filename, **kwargs)

        # Now see if there is an accompanying metadata file.  If so,
        # then merge those attributes into the config
        yaml_filename = f"{filename}.yaml"
        if os.path.exists(yaml_filename):
            dict = {}
            if config:
                dict = config.dictionary

            with open(yaml_filename, 'r') as file:
                new_dict = yaml.safe_load(file)
                dict.update(new_dict)

            config = Config(new_dict)

        # TODO: Use config?
        return xr.Dataset(dataframe.to_xarray())

    @staticmethod
    def variable_to_dict(ds: xr.Dataset, variable_name):
        var_dict = {}
        attributes = {}
        variable: xr.DataArray = ds[variable_name]

        # First save the attributes
        for attr in variable.attrs:
            attributes[attr] = variable.attrs.get(attr)
        var_dict['attrs'] = attributes

        # Now save the dimension information
        if DSUtil.is_coord_var(ds, variable_name):
            var_dict['coodinate_variable'] = True
        else:
            dims, lengths = DSUtil.get_shape(ds, variable_name)
            var_dict['dims'] = dims

        return var_dict

@register_filehandler(".csv")
class CSVHandler(FileHandler):
    def write(self, dataset: xr.Dataset, filename: str, **kwargs):
        dataframe = dataset.to_dataframe()
        dataframe.to_csv(filename)
        # Config.from(dataset).save(f"{filename}.yaml")
    def read(self, filename: str):
        pass

FILEHANDLERS: Dict[str, FileHandler] = FILEHANDLERS
