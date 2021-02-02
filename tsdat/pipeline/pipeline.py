import abc
import xarray as xr
from typing import Dict
from tsdat.config import Config
from tsdat.io.storage import DatastreamStorage


class Pipeline(abc.ABC):

    def __init__(self, config: Config = None, storage: DatastreamStorage = None) -> None:
        self.storage = storage
        self.config = config

    @abc.abstractmethod
    def run(self, filepath: str):
        return

    def standardize_dataset(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Standardizes the dataset by applying variable name and units 
        conversions as defined in the config. Returns the standardized 
        dataset.

        Args:
            dataset (xr.Dataset):   The raw xarray dataset.

        Returns:
            xr.Dataset: The standardized dataset.
        -------------------------------------------------------------------"""
        definition = self.config.dataset_definition
        
        # Add the input_files attribute to global attributes
        definition.add_input_files_attr(list(raw_mapping.keys()))

        for coordinate in definition.coords.values():
            definition.extract_data(coordinate, dataset)
        
        for variable in definition.vars.values():
            definition.extract_data(variable, dataset)

        standardized_dataset = xr.Dataset.from_dict(definition.to_dict())

        return standardized_dataset
    
    def validate(self, dataset: xr.Dataset):
        """-------------------------------------------------------------------
        Confirms that the dataset conforms with MHKiT-Cloud data standards. 
        Raises an error if the dataset is improperly formatted. This method 
        should be overridden if different standards or validation checks 
        should be applied.

        Args:
            dataset (xr.Dataset): The dataset to validate.
        -------------------------------------------------------------------"""
        pass
