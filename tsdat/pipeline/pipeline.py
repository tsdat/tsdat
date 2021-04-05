import abc
import datetime
import numpy as np
import xarray as xr
from typing import Any, Dict

from tsdat.utils import DSUtil
from tsdat.config import (
    Config,
    DatasetDefinition,
)
from tsdat.io import (
    DatastreamStorage,
    FileHandler
)


class Pipeline(abc.ABC):

    def __init__(self, config: Config = None, storage: DatastreamStorage = None) -> None:
        self.storage = storage
        self.config = config

    @abc.abstractmethod
    def run(self, filepath: str):
        return

    def standardize_dataset(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Standardizes the dataset by applying variable name and units 
        conversions as defined in the config. Returns the standardized 
        dataset.

        Args:
            raw_mapping (Dict[str, xr.Dataset]):   The raw xarray dataset mapping.

        Returns:
            xr.Dataset: The standardized dataset.
        -------------------------------------------------------------------"""
        definition = self.config.dataset_definition
        
        raw_datasets = self.reduce_raw_datasets(raw_mapping, definition)

        # Merge only preserves global attributes from the first dataset.
        # Variable attributes are only preserved from the first dataset.
        merged_dataset = xr.merge(raw_datasets)

        # Check required variables are in merged dataset
        self.check_required_variables(merged_dataset, definition)

        # Ensure all variables are initialized. Computed variables, variables
        # that are set statically, and variables that weren't retrieved should
        # be initialized.
        merged_dataset = self.add_static_variables(merged_dataset, definition) 
        merged_dataset = self.add_missing_variables(merged_dataset, definition)

        # Add global and variable attributes to dataset
        merged_dataset = self.add_attrs(merged_dataset, raw_mapping, definition)

        return merged_dataset

    def check_required_variables(self, dataset: xr.Dataset, dod: DatasetDefinition):
        """-------------------------------------------------------------------
        Function to throw an error if a required variable could not be 
        retrieved.

        Args:
        ---
            dataset (xr.Dataset): The dataset to check.
            dod (DatasetDefinition): The DatasetDefinition used to specify 
                                        required variables.

        Raises:
        ---
            Exception: Raises an exception to indicate the variable could not 
                        be retrieved.
        -------------------------------------------------------------------"""
        for variable in dod.coords.values():
            if variable.is_required() and variable.name not in dataset.variables:
                raise Exception(f"Required coordinate variable '{variable.name}' could not be retrieved.")
        for variable in dod.vars.values():
            if variable.is_required() and variable.name not in dataset.variables:
                raise Exception(f"Required variable '{variable.name}' could not be retrieved.")

    def add_static_variables(self, dataset: xr.Dataset, dod: DatasetDefinition) -> xr.Dataset:
        """-------------------------------------------------------------------
        Uses the dataset definition to add static variables (variables that 
        are hard-coded in the config) to the output dataset.

        Args:
        ---
            dataset (xr.Dataset): The dataset to add the variables to
            dod (DatasetDefinition): The DatasetDefinition object to pull data from.

        Returns:
        ---
            xr.Dataset: The original dataset with added variables from the config.
        -------------------------------------------------------------------"""
        coords, data_vars = {}, {}
        for variable in dod.get_static_variables():
            if variable.is_coordinate():
                coords[variable.name] = variable.to_dict()
            else:
                data_vars[variable.name] = variable.to_dict()
        static_ds = xr.Dataset.from_dict({"coords": coords, "data_vars": data_vars})
        return xr.merge([dataset, static_ds])

    def add_missing_variables(self, dataset: xr.Dataset, dod: DatasetDefinition):
        """-------------------------------------------------------------------
        Uses the dataset definition to initialize variables that are defined
        in the dataset definition but did not have input. Uses the appropriate
        shape and _FillValue to initialize each variable.

        Args:
        ---
            dataset (xr.Dataset): The dataset to add the variables to
            dod (DatasetDefinition): The DatasetDefinition to use.

        Returns:
        ---
            xr.Dataset: The original dataset with possible additional variables initialized.
        -------------------------------------------------------------------"""
        coords, data_vars = {}, {}
        for var_name, var_def in dod.vars.items():
            if var_name not in dataset.variables:
                if var_def.is_coordinate():
                    coords[var_name] = var_def.to_dict()
                    shape = dod.dimensions[var_name].length
                    coords[var_name]["data"] = np.full(shape, var_def.get_FillValue())
                else:
                    data_vars[var_name] = var_def.to_dict()
                    shape = [len(dataset[dim_name]) for dim_name in var_def.dims.keys()]
                    data_vars[var_name]["data"] = np.full(shape, var_def.get_FillValue())

        missing_vars_ds = xr.Dataset.from_dict({"coords": coords, "data_vars": data_vars})
        return xr.merge([dataset, missing_vars_ds])

    def add_attrs(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset], dod: DatasetDefinition) -> xr.Dataset:
        """-------------------------------------------------------------------
        Adds global and variable-level attributes to the dataset from the 
        DatasetDefinition.

        Args:
            dataset (xr.Dataset): The dataset to add attributes to.
            raw_mapping (Dict[str, xr.Dataset]): The raw dataset mapping. Used
                                                to set the 'input_files' global 
                                                attribute.
            dod (DatasetDefinition): The DatasetDefinition containing the 
                                    attributes to add.

        Returns:
            xr.Dataset: The original dataset with the attributes added.
        -------------------------------------------------------------------"""
        def _set_attr(obj: Any, att_name: str, att_val: Any):
            if hasattr(obj, "attrs"):
                if hasattr(obj.attrs, att_name):
                    prev_val = obj.attrs[att_name]
                    UserWarning(f"Warning: Overriding attribute {att_name}. Previously was '{prev_val}'")
                obj.attrs[att_name] = att_val
            else:
                UserWarning(f"Warning: Object {str(obj)} has no 'attrs' attribute.")

        for att_name, att_value in dod.attrs.items():
            _set_attr(dataset, att_name, att_value)

        for coord, coord_def in dod.coords.items():
            for att_name, att_value in coord_def.attrs.items():
                _set_attr(dataset[coord], att_name, att_value)

        for var, var_def in dod.vars.items():
            for att_name, att_value in var_def.attrs.items():
                _set_attr(dataset[var], att_name, att_value)
        
        dataset.attrs["input_files"] = list(raw_mapping.keys())
        
        history = f"Ran at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.attrs["history"] = history

        return dataset

    def get_previous_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Utility method to retrieve the previous set of data for the same 
        datastream as the provided dataset from the DatastreamStorage.

        Args:
        ---
            dataset (xr.Dataset):   The reference dataset that will be used to
                                    search the DatastreamStore for prior data.

        Returns:
        ---
            xr.Dataset: The previous dataset from the DatastreamStorage if it
                        exists, else None.
        -------------------------------------------------------------------"""
        prev_dataset = None
        start_date, start_time = DSUtil.get_start_time(dataset)
        datastream_name = DSUtil.get_datastream_name(dataset, self.config)

        with self.storage.tmp.fetch_previous_file(datastream_name, f'{start_date}.{start_time}') as netcdf_file:
            if netcdf_file:
                prev_dataset = FileHandler.read(netcdf_file, config=self.config)

        return prev_dataset
    
    def store_and_reopen_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Writes the dataset to a local netcdf file and then uses the 
        DatastreamStorage object to persist it. 

        Args:
        ---
            dataset (xr.Dataset): The dataset to store.
        
        Returns:
        ---
            xr.Dataset: The dataset after it has been saved to disk and 
                        reopened.
        -------------------------------------------------------------------"""
        reopened_dataset = None

        # TODO: modify storage.save so it can take a dataset or a file path as
        # parameter.  If a dataset is passed, then move the below code to
        # storage to save the dataset for all registered outputs.
        # If a file path is passed, then just perform the storage save is it is now.
        #self.storage.save(dataset)
        #self.storage.save(tmp_path)

        with self.storage.tmp.get_temp_filepath(DSUtil.get_dataset_filename(dataset)) as tmp_path:
            FileHandler.write(dataset, tmp_path)
            self.storage.save(tmp_path)
            reopened_dataset = xr.load_dataset(tmp_path)

        return reopened_dataset
