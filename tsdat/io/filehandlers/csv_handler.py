import os
import yaml
import pandas as pd
import xarray as xr
from tsdat.config import Config
from tsdat.utils import DSUtil
from .file_handlers import AbstractFileHandler
from .file_handlers import register_filehandler

@register_filehandler(".csv")
class CsvHandler(AbstractFileHandler):

    @staticmethod
    def write(ds: xr.Dataset, filename: str, config: Config = None, **kwargs):
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
            dictionary = {
                "dims":     [name for name, dim in variable.dims.items()],
                "attrs":    {attr_name: attr.value for attr_name, attr in variable.attrs.items()}
            }
            variables[variable_name] = dictionary

        # Then save the DataFrame to a csv file
        kwargs['index'] = False
        df.to_csv(filename, **kwargs)

        # Now save all the metadata dictionary to a companion yaml file
        metadata = {"variables": variables}
        yaml_filename = f"{filename}.yaml"
        with open(yaml_filename, 'w') as file:
            yaml.dump(metadata, file)
    
    @staticmethod
    def read(filename: str, **kwargs):
        # First read the csv into a pandas dataframe
        # dataset_def = DSUTIL.extract_metadata(ds)
        # ds = DSUTIL.set_metadata(ds, dataset_definition)
        # or could live in Config/DatasetDefiniton (Or both)
        dataframe: pd.DataFrame = pd.read_csv(filename, **kwargs)

        # Now see if there is an accompanying metadata file.  If so,
        # then merge those attributes into the config
        yaml_filename = f"{filename}.yaml"
        if os.path.exists(yaml_filename):
            dict = {}

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
