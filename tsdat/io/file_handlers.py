from abc import abstractmethod
import os
import xarray
import pandas
import yaml
import numpy as np
from tsdat import TimeSeriesDataset, Config


class FileHandler:

    @abstractmethod
    def write(self, ds: TimeSeriesDataset, filename: str, **kwargs):
        """
        Save the given dataset to file
        :param ds: The dataset to save
        :param filename: An absolute or relative path to the file including filename
        """
        pass

    @abstractmethod
    def read(self, filename: str, config: Config = None, **kwargs):
        """
         Read the given file into a TimeSeriesDataset
        :param filename:
        :param config: optional tsdat Config
        :return: The dataset
        :rtype: TimeSeriesDataset
        """
        pass


class NetCdfHandler(FileHandler):

    def write(self, ds: TimeSeriesDataset, filename: str, **kwargs):
        ds.xr.to_netcdf(filename, format='NETCDF4')

    def read(self, filename: str, config: Config = None):
        # TODO: need to have TimeSeriesDataset close the file automatically if user
        #  uses "with" - add a resource manager api
        ds_disk = xarray.open_dataset(filename)
        return TimeSeriesDataset(ds_disk, config)


class CsvHandler(FileHandler):

    def write(self, ds: TimeSeriesDataset, filename: str, **kwargs):
        # You can only write one-dimensional data to csv
        if len(ds.xr.dims) > 1:
            raise TypeError("Dataset has more than one dimension, so it can't be saved to csv.  Try netcdf instead.")

        # First convert the data to a Pandas DataFrame and
        # save the variable metadata in a dictionary
        variables = {}
        df: pandas.DataFrame = pandas.DataFrame()
        for variable_name in ds.xr.variables:
            variable = ds.get_var(variable_name)
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
        dataframe: pandas.DataFrame = pandas.read_csv(filename, **kwargs)

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

        return TimeSeriesDataset(dataframe.to_xarray(), config)

    @staticmethod
    def variable_to_dict(ds: TimeSeriesDataset, variable_name):
        var_dict = {}
        attributes = {}
        variable: xarray.DataArray = ds.get_var(variable_name)

        # First save the attributes
        for attr in variable.attrs:
            attributes[attr] = variable.attrs.get(attr)
        var_dict['attrs'] = attributes

        # Now save the dimension information
        if ds.is_coord_var(variable_name):
            var_dict['coodinate_variable'] = True
        else:
            dims, lengths = ds.get_shape(variable_name)
            var_dict['dims'] = dims

        return var_dict

