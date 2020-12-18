from typing import List
import xarray as xr
from tsdat import Config, DatastreamStorage


class Pipeline:
    def __init__(self, config:Config, storage:DatastreamStorage) -> None:        
        self.storage = storage
        self.config = config
        pass

    def run(self, filepath:str):
        raise NotImplementedError
 
 
class IngestPipeline(Pipeline):       

    def run(self, filepath: str):
        # Convert the raw files 
        file_paths = self.extract_files(filepath)
        raw_files = self.rename_raw_files(file_paths)
        
        # Process the data
        raw_dataset = self.read_input(file_paths)
        dataset = self.standardize_dataset(raw_dataset)
        dataset = self.apply_corrections(dataset)
        dataset = self.customize_dataset(dataset)
        
        if self.config.pipeline.data_level.startswith('b'):
            dataset = self.qc_dataset(dataset)
        
        # See if some data already exists in Storage for the same day
        # If so, then we need to pull in the remote data and merge with
        # our new data.
        dataset = self.merge_existing_data(dataset)
        
        # Save the final datastream data to storage
        self.store_dataset(dataset)
        self.store_raw(raw_files)
        
    def extract_files(self, filepath: str):
        # If file is an archive, then unzip and return
        # the list of file paths to each file
        pass
 
    def rename_raw_files(self, file_paths: List[str]):
        for file_path in file_paths:
            # Rename the local file to standard name for raw data
            pass
        # Return the list of renamed files
        
    def read_input(self, file_paths: List[str] ):
        for file_path in file_paths:
            
            # Our reader/writer registry should be smart enough
            # to infer the format from the file extension.
            # If there is no reader defined for the given file ext,
            # then raise an error.
            
            # Read each file into the same xarray dataset
            pass
        
        # return xarray dataset
    
    def standardize_dataset(self, raw_dataset: xr.Dataset):
        """-------------------------------------------------------------
        
         Args:
            raw_dataset (xr.Dataset): [description]
        --------------------------------------------------------------"""
        # Use the config object to map the raw dataset variable names
        # to the output dataset.
        # Make sure the output dataset conforms to CF standards.
        # For each variable in the output dataset, convert to the 
        # standard units defined in the config.add()
        
        # Return the output dataset
        pass
    
    def apply_corrections(self, dataset: xr.Dataset):
        # This is a placeholder hook that can be used to 
        # apply standard corrections for the instrument/measurement or calibrations.
        # It can also be used to insert any derived properties
        # into the dataset.
        
        # Return the dataset with corrections/calibrations applied
        pass
    
        
    def qc_dataset(self, dataset: xr.Dataset):
        # Apply the qc tests defined in the config
        # Some qc tests (such as on a coordinate variable),
        # may raise an exception, causing the pipeline to fail.
        pass
    
    def merge_existing_data(self, dataset: xr.Dataset):
        # See if some data already exists in Storage for the same day
        # If so, then we need to pull in the remote data and merge with
        # our new data.
        pass
        
    def store_dataset(self, dataset: xr.Dataset):
        # This method should write the dataset to a local netcdf file
        # and then use the DatastreamStorage object to persist it.
        # The file path should be automatically determined from the config
        dataset_file_path = self.get_dataset_filepath(dataset)
        pass

    def store_raw(raw_file_paths: List[str]):
        for file_path in raw_file_paths:
            pass
            # Our reader/writer registry should be smart enough
            # to infer the format from the file extension.
            # If there is no reader defined for the given file ext,
            # then raise an error.
    
    def get_dataset_filepath(self, dataset: xr.Dataset):
        # Use the times from the dataset and the datastream
        # name to generate a file path relative to the 
        # storage root
        datastream_name = self.get_datastream_name()
        
    def get_datastream_name(self):
        # Use the config pipeline parameters to auto generate the
        # datastream name
        pass