import os
import abc
import act
import numpy as np
import xarray as xr
import datetime as dt
from typing import Dict
from tsdat.config import Config
from tsdat.standards import Standards
from tsdat.io.storage import DatastreamStorage
from tsdat.io.filehandlers import FileHandler


class Pipeline(abc.ABC):

    def __init__(self, config: Config = None, storage: DatastreamStorage = None) -> None:
        self.storage = storage
        self.config = config

    @abc.abstractmethod
    def run(self, filepath: str):
        return

    def standardize(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Uses the config object to map the input dataset variable names to the 
        output dataset. Additionally, this function ensures that the dataset 
        conforms to MHKiT-Cloud data standards. 

        Args:
            dataset (xr.Dataset):   The raw data into an xarray dataset.
            config (Config): The config object associated with the dataset.

        Returns:
            xr.Dataset: The standardized dataset.
        -------------------------------------------------------------------"""
        dod = self.config.dataset_definition
        data_dict = dod.to_dict()

        # Get coordinate variable dimensions. This is used to initialize the 
        # data array for each variable
        coord_sizes: Dict[str, int] = {}
        for coord_name, coord_var_def in dod.coords.items():
            input_name = coord_name
            if coord_var_def.input: 
                input_name = coord_var_def.input.name
            coord_var = dataset[input_name]
            shape = coord_var.data.shape
            if len(shape) != 1:
                raise ValueError(f"Coordinate variable {coord_name} must be one-dimensional")
            coord_sizes[coord_name] = shape[0]

        # Populate dataset dictionary for coordinate variables
        for coord_name, coord_var_def in dod.coords.items():

            # Create array of correct size with _FillValue
            dim_names = coord_var_def.dims.keys()
            dim_shapes = tuple([coord_sizes[dim_name] for dim_name in dim_names])
            _FillValue_ATT = coord_var_def.attrs.get("_FillValue", {})
            _FillValue = -9999
            if _FillValue_ATT:
                _FillValue = _FillValue_ATT.value
            data = np.empty(dim_shapes, dtype=coord_var_def.type)
            data.fill(_FillValue)

            # If variable should be copied from input, do that.
            var_name = coord_name
            if coord_var_def.input:
                input_name = coord_var_def.input.name
                input_data = dataset[input_name].data
                # input_data = np.array(dataset[input_name].data, dtype=coord_var_def.type)
                if coord_var_def.input.time_format:  
                    # Use time_format as input to datetime.datetime.strptime
                    # Convert datetime.datetime object to np.datetime64 using act
                    time_format = coord_var_def.input.time_format
                    datetimes = [dt.datetime.strptime(time, time_format) for time in input_data]
                    # TODO: use pandas.to_datetime(*) to convert. 
                    # TODO: Do unit conversion if necessary
                    input_data = np.array(datetimes, dtype='datetime64[s]') # TODO: Check to see if using seconds instead of default (ms) helps ds.to_netcdf() later
                data = input_data
            
            # Add data to the data dictionary for the current variable
            data_dict["coords"][coord_name]["data"] = data
        
        # Populate dataset dictionary for regular variables
        for var_name, var_def in dod.vars.items():

            # Create array of correct size with _FillValue
            dim_names = var_def.dims.keys()
            dim_shapes = tuple([coord_sizes[dim_name] for dim_name in dim_names])
            data = np.empty(dim_shapes, dtype=var_def.type)
            _FillValue_ATT = var_def.attrs.get("_FillValue", {})
            _FillValue = -9999
            if _FillValue_ATT:
                _FillValue = _FillValue_ATT.value
            data = np.empty(dim_shapes, dtype=var_def.type)
            data.fill(_FillValue)

            # If variable should be copied from input do that.
            if var_def.input:
                input_name = var_def.input.name
                input_data = np.array(dataset[input_name].data, dtype=var_def.type)
                # TODO: Don't copy NaN values?
                data = input_data
            
            # Add data to the data dictionary for the current variable
            if not data_dict["data_vars"][var_name]["data"]:
                data_dict["data_vars"][var_name]["data"] = data
        
        # Create the dataset from our constructed data dictionary
        formatted_dataset = xr.Dataset.from_dict(data_dict)

        # for variable in dod.vars["variables"]:
            # get original values
            # convert to formatted values
            # add the values back to the dict
        
        # dataset = xr.Dataset(definition)
        Standards.validate(dataset)
        return formatted_dataset
    
    def validate_dataset(self, dataset: xr.Dataset):
        """-------------------------------------------------------------------
        Confirms that the dataset conforms with MHKiT-Cloud data standards. 
        Raises an error if the dataset is improperly formatted. This method 
        should be overridden if different standards or validation checks 
        should be applied.

        Args:
            dataset (xr.Dataset): The dataset to validate.
        -------------------------------------------------------------------"""
        Standards.validate(dataset)
 