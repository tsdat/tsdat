import os
import abc
import shutil
import zipfile
import xarray as xr
from typing import List
from tsdat import Config, DatastreamStorage


class Pipeline(abc.ABC):

    def __init__(self, config:Config, storage:DatastreamStorage) -> None:        
        self.storage = storage
        self.config = config
        pass

    @abc.abstractmethod
    def run(self, filepath:str):
        return
 
 
class IngestPipeline(Pipeline):       

    def run(self, filepath: str) -> None:
        """-------------------------------------------------------------------
        Runs the Ingest Pipeline from start to finish.

        Args:
            filepath (str): The path to the file (or .zip archive containing 
                            a collection of files) to run the Ingest Pipeline 
                            on.
        -------------------------------------------------------------------"""        
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
        
    def extract_files(self, filepath: str, target_dir: str = "") -> List[str]:        
        """-------------------------------------------------------------------
        If provided a path to a .zip archive this function will unzip the 
        archive and return a list of complete paths to each file.

        Args:
            filepath (str): A path to a .zip file or a regular file.
            target_dir (str, optional): A path to the directory to extract the 
                                        files to. Defaults to the parent 
                                        directory of `filepath`.

        Returns:
            List[str]:  A list of complete paths to the unzipped files or 
                        `[filepath]` if `filepath` is not a .zip file.
        -------------------------------------------------------------------"""        
        pass
    
    # Rename the local files to standard name for raw data
    # and return the list of renamed files
    def rename_raw_files(self, file_paths: List[str]) -> List[str]:
        """-------------------------------------------------------------------
        Renames the provided RAW files according to MHKiT-Cloud Data Standards 
        naming conventions for RAW data and returns a list of filepaths to the 
        renamed files.

        Args:
            file_paths (List[str]): A list of paths to the original raw files.

        Returns:
            List[str]: A list of paths to the renamed raw files.
        -------------------------------------------------------------------"""    
        pass
        
    def read_input(self, file_paths: List[str]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Concatenates data from a number of file paths into an xarray dataset 
        and returns the xarray dataset. The input files can have any file 
        extension so long as a FileReader for that file type has been 
        registered.

        Args:
            file_paths (List[str]): A list of paths to input files
        
        Returns:
            xr.Dataset: An xarray dataset containing the raw input data. No 
                        qc, corrections, or standard format check/controls 
                        have been applied.
        -------------------------------------------------------------------"""
        for file_path in file_paths:
            
            # Our reader/writer registry should be smart enough
            # to infer the format from the file extension.
            # If there is no reader defined for the given file ext,
            # then raise an error.
            
            # Read each file into the same xarray dataset
            pass
        
        # return xarray dataset
    
    def standardize_dataset(self, raw_dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Uses the config object to map the raw dataset variable names to the 
        output dataset. Additionally, this function ensures that the dataset 
        conforms to MHKiT-Cloud data standards. 

        Args:
            raw_dataset (xr.Dataset):   The raw data ingested into an xarray 
                                        dataset.

        Returns:
            xr.Dataset: The standardized dataset.
        -------------------------------------------------------------------"""
        # Use the config object to map the raw dataset variable names
        # to the output dataset.
        # Make sure the output dataset conforms to CF standards.
        # For each variable in the output dataset, convert to the 
        # standard units defined in the config.add()
        
        # Return the output dataset
        pass
    
    def apply_corrections(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Pipeline hook that can be used to apply standard corrections for the 
        instrument/measurement or calibrations. It can also be used to insert 
        any derived properties into the dataset. This method is called 
        immediately after the dataset is converted to standard format.

        Args:
            dataset (xr.Dataset):   A standardized xarray dataset where the 
                                    variable names correspond with the output 
                                    variable names from the config file.
        Returns:
            xr.Dataset: The input xarray dataset with corrections applied.
        -------------------------------------------------------------------"""
        # This is a placeholder hook that can be used to 
        # apply standard corrections for the instrument/measurement or calibrations.
        # It can also be used to insert any derived properties
        # into the dataset.
        
        # Return the dataset with corrections/calibrations applied
        pass
    
        
    def qc_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Applies the qc tests defined in the config. Some qc tests (such as 
        those applied to a coordinate variable) may raise an exception, 
        causing the pipeline to fail.

        Args:
            dataset (xr.Dataset):   The input dataset, converted to standard 
                                    format and with corrections applied.
        Returns:
            xr.Dataset: The input xarray dataset with qc checks applied.
        -------------------------------------------------------------------"""
        # Apply the qc tests defined in the config
        # Some qc tests (such as on a coordinate variable),
        # may raise an exception, causing the pipeline to fail.
        pass
    
    def merge_existing_data(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Checks the DatastreamStorage to see if data already exists for the 
        same day. If so, then this function fetches that data and merges it 
        with the current dataset locally.

        Args:
            dataset (xr.Dataset):   The input dataset converted to standard 
                                    format with corrections and qc checks 
                                    applied.

        Returns:
            xr.Dataset: The input xarray dataset merged with existing data for
                        the same processed day.
        -------------------------------------------------------------------"""
        # See if some data already exists in Storage for the same day
        # If so, then we need to pull in the remote data and merge with
        # our new data.
        pass
        
    def store_dataset(self, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Writes the dataset to a local netcdf file and then uses the 
        DatastreamStorage object to persist it. 

        Args:
            dataset (xr.Dataset): The dataset to store.
        -------------------------------------------------------------------"""
        # This method should write the dataset to a local netcdf file
        # and then use the DatastreamStorage object to persist it.
        # The file path should be automatically determined from the config
        dataset_file_path = self.get_dataset_filepath(dataset)
        pass

    def store_raw(raw_file_paths: List[str]) -> None:
        """-------------------------------------------------------------------
        Uses the DatastreamStorage object to persist the raw "00"-level files.

        Args:
            raw_file_paths (List[str]): A list of paths to the raw files to 
                                        store.
        -------------------------------------------------------------------"""
        for file_path in raw_file_paths:
            pass
            # Our reader/writer registry should be smart enough
            # to infer the format from the file extension.
            # If there is no reader defined for the given file ext,
            # then raise an error.
    
    def get_dataset_filepath(self, dataset: xr.Dataset) -> str:
        """-------------------------------------------------------------------
        Uses the times from the dataset and the datastream name to generate a 
        file path relative to the storage root where the dataset should be 
        saved.

        Args:
            dataset (xr.Dataset): The xarray dataset whose filepath should be 
            generated.

        Returns:
            str:    The file path relative to the storage root where the 
                    dataset should be saved.
        -------------------------------------------------------------------"""
        # Use the times from the dataset and the datastream
        # name to generate a file path relative to the 
        # storage root
        datastream_name = self.get_datastream_name()
        
    def get_datastream_name(self) -> str:
        """-------------------------------------------------------------------
        Uses the config pipeline parameters to autogenerate the datastream 
        name according to MHKiT-Cloud data standards.

        Returns:
            str: The datastream name.
        -------------------------------------------------------------------"""
        # Use the config pipeline parameters to auto generate the
        # datastream name
        pass
    
    def get_datastream_date(self, filepath: str) -> str:
        """-------------------------------------------------------------------
        Given the path to a raw "00"-level file, this function returns the 
        date (yyyymmdd) pertaining to the first time sample in the file.

        Args:
            filepath (str): The path to a raw file.

        Returns:
            str:    The date of the first point in the file, in 'yyyymmdd' 
                    format.
        -------------------------------------------------------------------"""
        # Use the config pipeline parameters and the filepath 
        # to retrieve the date from the filepath
        pass

    def get_datastream_time(self, filepath: str) -> str:
        """-------------------------------------------------------------------
        Given the path to a raw "00"-level file, this function returns the 
        time (hhmmss) pertaining to the first time sample in the file.

        Args:
            filepath (str): The path to a raw file.

        Returns:
            str:    The time of the first point in the file, in 'hhmmss'
                    format.
        -------------------------------------------------------------------"""
        # Use the config pipeline parameters and the filepath 
        # to retrieve the time from the filepath
        pass