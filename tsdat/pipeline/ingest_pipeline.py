import os
import shutil
import zipfile
import xarray as xr
from typing import List, Tuple
from tsdat.standards import Standards
from .pipeline import Pipeline
from tsdat.utils import DSUtil
from tsdat.qc import QC


class IngestPipeline(Pipeline):       

    def run(self, filepath: str) -> None:
        """-------------------------------------------------------------------
        Runs the Ingest Pipeline from start to finish.

        Args:
            filepath (str): The path to the file (or .zip archive containing 
                            a collection of files) to run the Ingest Pipeline 
                            on.
        -------------------------------------------------------------------"""
        file_paths = self.extract_files(filepath)

        try:
            # Process the data
            raw_dataset = self.read_input(file_paths)
            dataset = self.standardize_dataset(raw_dataset)
            dataset = self.apply_corrections(dataset)
            dataset = self.customize_dataset(dataset)
            datastream_name = DSUtil.get_datastream_name(dataset)
            start_time = DSUtil.dataset_accessor.get_start_time(dataset)
            end_time = DSUtil.dataset_accessor.get_end_time(dataset)

            self.validate_dataset(dataset) # standards class, will fail if ds is not valid

            if self.config.pipeline.data_level.startswith('b'):
                # If there is previous data in Storage, we need
                # to load up the last file so we can perform
                # continuity checks such as monontonically increasing
                previous_dataset = self.get_previous_dataset(datastream_name, start_time)
                dataset = QC.apply_tests(dataset, self.config, previous_dataset)

            # See if some data already exists in Storage for the same day
            # If so, then we need to pull in the remote data and merge with
            # our new data.
            dataset = self.merge_existing_data(dataset)

            # Save the final datastream data to storage
            self.store_dataset(dataset)

        finally:
            # Standardize the raw file names and store
            raw_files = self.rename_raw_files(file_paths, datastream_name, start_time, end_time)
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
        if not filepath.endswith(".zip"):
            if os.path.isfile(filepath):
                return [filepath]
            raise ValueError("filepath must be a .zip archive or a file")
        # If target_dir not provided, make it be the parent directory of the 
        # filepath provided
        if not target_dir:
            loc = filepath.rfind("/")
            if loc != -1:
                target_dir = filepath[:loc]
            else:
                target_dir = "."
        # Extract into a temporary folder in the target_dir
        temp_dir = f"{target_dir}/.unzipped"
        os.makedirs(temp_dir, exist_ok=False)
        with zipfile.ZipFile(filepath, 'r') as zipped:
            zipped.extractall(temp_dir)
        # Move files from temp_dir into target_dir and remove temp_dir
        filenames = os.listdir(temp_dir)
        temp_paths = [os.path.join(temp_dir,   file) for file in filenames]
        new_paths  = [os.path.join(target_dir, file) for file in filenames]
        for temp_path, new_path in zip(temp_paths, new_paths):
            shutil.move(temp_path, new_path)
        os.rmdir(temp_dir)
        return new_paths
    
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
        renamed = []
        for file_path in file_paths:
            datastream_name = self.get_datastream_name()[:-2] + "00"
            date, time = self.get_start_date_time(file_path)
            new_path = f"{datastream_name}.{date}.{time}.raw.{file_path}"
            shutil.rename(file_path, new_path)
            renamed.append(new_path)
        return renamed
    
    def read_input(self, file_paths: List[str]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Merges data from a number of file paths into an xarray dataset and
        returns the xarray dataset. The input files can have any file 
        extension so long as a FileReader for that extension has been 
        registered.

        Args:
            file_paths (List[str]): A list of paths to raw input files. Can be
                                    provided as a string if there is only one 
                                    raw file to read in.
        
        Raises:
            KeyError:   Raises a KeyError if no FileHandler has been
                        registered for the extension of any raw files in 
                        `file_paths`.

        Returns:
            xr.Dataset: An xarray dataset containing the raw input data. No 
                        qc, corrections, or standard format check/controls 
                        have been applied.
        -------------------------------------------------------------------"""
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        merged_dataset = xr.Dataset()
        for file_path in file_paths:
            handler = self.get_filehandler(file_path)
            dataset = handler.read(file_path)
            merged_dataset = xr.merge([merged_dataset, dataset])            
        return merged_dataset
    
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
        return Standards.standardize(raw_dataset, self.config)
    
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
        # TODO: write this method
        return dataset
    
    def get_previous_dataset(self, datastream_name: str, start_time: str) -> xr.Dataset:
        end = start_time
        start = None
        # TODO start = start_time - 1 day
        netcdf_files = self.storage.fetch(datastream_name, start, end)
        netcdf_file = netcdf_files[-1] if len(netcdf_files) > 0 else None
        dataset = None
        if netcdf_file is not None:
            # TODO: If file exists, use file handler to open this netcdf file to a dataset
            dataset = None

        return dataset
    
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
        # TODO: write this method
        return dataset
        
    def store_dataset(self, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Writes the dataset to a local netcdf file and then uses the 
        DatastreamStorage object to persist it. 

        Args:
            dataset (xr.Dataset): The dataset to store.
        -------------------------------------------------------------------"""
        # TODO: write to a better location locally
        local_path = f".{self.get_datastream_name()}.temp.nc"
        dataset_file_path = self.get_dataset_filepath(dataset)
        dataset.to_netcdf(local_path)
        self.storage.save(local_path, dataset_file_path)
        os.remove(local_path)

    def store_raw(self, raw_file_paths: List[str]) -> None:
        """-------------------------------------------------------------------
        Uses the DatastreamStorage object to persist the raw "00"-level files.

        Args:
            raw_file_paths (List[str]): A list of paths to the raw files to 
                                        store.
        -------------------------------------------------------------------"""
        for file_path in raw_file_paths:
            self.storage.save(file_path, file_path)
        return
    
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

    def get_dataset_filename(self, dataset: xr.Dataset) -> str:
        """-------------------------------------------------------------------
        Given an xarray dataset this function will return the base filename of
        the dataset according to MHkiT-Cloud data standards. The base filename 
        does not include the directory structure where the file should be 
        saved, only the name of the file itself, e.g.
        z05.ExampleBuoyDatastream.b1.20201230.000000.nc

        Args:
            dataset (xr.Dataset):   The dataset whose filename should be 
                                    generated.

        Returns:
            str: The base filename of the dataset.
        -------------------------------------------------------------------"""
        # TODO: write this method
        return ""
     
    def get_datastream_name(self) -> str:
        """-------------------------------------------------------------------
        Uses the config pipeline parameters to autogenerate the datastream 
        name according to MHKiT-Cloud data standards.

        Returns:
            str: The datastream name.
        -------------------------------------------------------------------"""
        # TODO: move this to Config
        loc_id = self.config.pipeline["location_id"]
        instr_id = self.config.pipeline["instrument_id"]
        qualifier = self.config.pipeline["qualifier"]
        temporal = self.config.pipeline["temporal"]
        data_level = self.config.pipeline["data_level"]
        return f"{loc_id}.{instr_id}{qualifier}{temporal}.{data_level}"
    
    def get_start_date_time(self, filepath: str) -> Tuple[str, str]:                
        """-------------------------------------------------------------------
        Given the path to a raw "00"-level file, this function returns the 
        date (yyyymmdd) and time (hhmmss) pertaining to the first time sample
        in the file.

        Args:
            filepath (str): The path to a raw file.

        Returns:
            Tuple[str, str]:    A 2-tuple of the timestamp of the first point
                                in the file with date (yyyymmdd) first and 
                                time (hhmmss) second.
        -------------------------------------------------------------------"""
        # Use the filepath and the appropriate filereader to open the raw file
        # and read the value of the first time sample in the file. The config
        # pipeline parameters will also need to be used to get the source name
        # and units of the time variable in the raw file. The date/time 
        # returned should be in UTC.
        # TODO: write this method
        return "99999999", "999999"
    
    def organize_files(self, file_paths: List[str]) -> List[str]:
        """-------------------------------------------------------------------
        Given a list of paths to files with filenames consistent with
        MHKiT-Cloud data standards naming conventions for RAW or processed 
        data, this method will move the provided files to the correct location
        on the local filesystem and return their new paths.

        Args:
            file_paths (List[str]): A list of paths to file. Can
                                                be a string if only one file
                                                is to be organized.

        Returns:
            List[str]: A list of paths to the organized files.
        -------------------------------------------------------------------"""
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        organized_filepaths = []
        for filepath in file_paths:
            if not os.path.isfile(filepath):
                raise ValueError(f"\"{filepath}\" is not a file")
            _, filename = os.path.split(filepath)
            components = filename.split(".")
            location_id = components[0]
            datastream_name = components[:3]
            new_dir = f"/data/{location_id}/{datastream_name}"
            new_filepath = os.path.join(new_dir, filename)
            os.makedirs(new_dir)
            shutil.move(filepath, new_filepath)
            organized_filepaths.append(new_filepath)
        return organized_filepaths
