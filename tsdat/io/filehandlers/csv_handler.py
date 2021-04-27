import os
import yaml
import pandas as pd
import xarray as xr
from tsdat.config import Config
from tsdat.utils import DSUtil
from .file_handlers import AbstractFileHandler


class CsvHandler(AbstractFileHandler):

    def write(self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs):
        if len(ds.dims) > 1:
            raise TypeError("Dataset has more than one dimension, so it can't be saved to csv.  Try netcdf instead.")

        write_params = self.parameters.get('write', {})
        to_dataframe_kwargs = write_params.get('to_dataframe', {})
        to_csv_kwargs = dict(index=False)
        to_csv_kwargs.update(write_params.get('to_csv', {}))

        df = ds.to_dataframe(**to_dataframe_kwargs)
        df.to_csv(filename, **to_csv_kwargs)

        yaml_filename = f"{filename}.yaml"
        with open(yaml_filename, 'w') as file:
            metadata = DSUtil.get_metadata(ds)
            yaml.dump(metadata, file)

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        read_params = self.parameters.get('read', {})
        read_csv_kwargs = read_params.get('read_csv', {})
        to_xarray_kwargs = read_params.get('to_xarray', {})

        df = pd.read_csv(filename, **read_csv_kwargs)
        ds: xr.Dataset = df.to_xarray(**to_xarray_kwargs)

        yaml_filename = f"{filename}.yaml"
        if os.path.isfile(yaml_filename):
            with open(yaml_filename, 'r') as file:
                metadata = yaml.safe_load(file)
                ds.attrs = metadata.get('attributes', {})
                for variable, attrs in metadata.get('variables', {}).items():
                    ds[variable].attrs = attrs

        return ds
