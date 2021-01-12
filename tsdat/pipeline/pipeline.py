import os
import abc
import xarray as xr
from tsdat.config import Config
from tsdat.standards import Standards
from tsdat.io.storage import DatastreamStorage
from tsdat.io.file_handlers import FILEHANDLERS, FileHandler


class Pipeline(abc.ABC):

    def __init__(self, config: Config, storage: DatastreamStorage) -> None:
        self.storage = storage
        self.config = config
        pass

    @abc.abstractmethod
    def run(self, filepath: str):
        return
    
    def get_filehandler(self, file_path: str) -> FileHandler:
        """-------------------------------------------------------------------
        Retrieves the appropriate FileHandler for a given file. 

        Args:
            file_path (str):    The complete path to the file requiring a 
                                FileHandler.

        Raises:
            KeyError:   Raises KeyError if no FileHandler has been defined for
                        the the file provided.

        Returns:
            FileHandler: The FileHandler class to use for the provided file.
        -------------------------------------------------------------------"""
        _, ext = os.path.splitext(file_path)
        if ext not in FILEHANDLERS:
            raise KeyError(f"no FileHandler for extension: {ext}")
        return FILEHANDLERS[ext]

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
        definition = self.config.dataset_definition.to_dict()
        
        # definition["attributes"]
        # definition["dimensions"]

        # for variable in definition["variables"]:
            # get original values
            # convert to formatted values
            # add the values back to the dict
        
        dataset = xr.Dataset(definition)
        Standards.validate(dataset)
        return dataset
    
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
 