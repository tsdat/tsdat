import warnings
import xarray as xr
from typing import Dict, List
from .pipeline import Pipeline
from tsdat.io.filehandlers import FileHandler
from tsdat.qc import QC
from tsdat.utils import DSUtil

from .pipeline import Pipeline


class IngestPipeline(Pipeline):       

    def run(self, filepath: str) -> None:
        """-------------------------------------------------------------------
        Runs the Ingest Pipeline from start to finish.

        Args:
        ---
            filepath (str): The path to the file (or archive containing
                            a collection of files) to run the Ingest Pipeline 
                            on.
        -------------------------------------------------------------------"""
        # If the file is a zip/tar, then we need to extract the individual files
        with self.storage.tmp.extract_files(filepath) as file_paths:

            # Open each raw file into a Dataset, standardize the raw file names and store.
            # Use storage and FileHandler to access and read the file.
            # Involves opening the raw
            # files to get the timestamp of the first point of data since this is
            # required/recommended by raw file naming conventions.
            raw_dataset_mapping: Dict[str, xr.Dataset] = self.read_and_persist_raw_files(file_paths)
            
            raw_dataset_mapping: Dict[str, xr.Dataset] = self.customize_raw_datasets(raw_dataset_mapping)

            # Process the data
            dataset = self.standardize_dataset(raw_dataset_mapping)
            dataset = self.apply_corrections(dataset, raw_dataset_mapping)
            dataset = self.customize_dataset(dataset, raw_dataset_mapping)

            if self.config.dataset_definition.get_attr('data_level').startswith('b'):
                # If there is previous data in Storage, we need
                # to load up the last file so we can perform
                # continuity checks such as monontonically increasing
                previous_dataset = self.get_previous_dataset(dataset)
                QC.apply_tests(dataset, self.config, previous_dataset)

            # Save the final datastream data to storage
            dataset = self.store_and_reopen_dataset(dataset)

            # Hook to generate plots
            # Users should save plots with self.storage.save(paths_to_plots)
            self.create_and_persist_plots(dataset)

        # Make sure that any temp files are cleaned up
        self.storage.tmp.clean()

    def read_and_persist_raw_files(self, file_paths: List[str]) -> List[str]:
        """-------------------------------------------------------------------
        Renames the provided RAW files according to MHKiT-Cloud Data Standards 
        naming conventions for RAW data and returns a list of filepaths to the 
        renamed files.

        Args:
        ---
            file_paths (List[str]): A list of paths to the original raw files.

        Returns:
        ---
            List[str]: A list of paths to the renamed raw files.
        -------------------------------------------------------------------"""
        raw_dataset_mapping = {}

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        for file_path in file_paths:

            # read the raw file into a dataset
            with self.storage.tmp.fetch(file_path) as tmp_path:
                dataset = FileHandler.read(tmp_path)

                # Don't use dataset if no FileHandler is registered for it
                if dataset is not None:
                    # create the standardized name for raw file
                    new_filename = DSUtil.get_raw_filename(dataset, tmp_path, self.config)

                    # add the raw dataset to our dictionary
                    raw_dataset_mapping[new_filename] = dataset

                    # save the raw data to storage
                    self.storage.save(tmp_path, new_filename)
                
                else:
                    warnings.warn(f"Couldn't use extracted raw file: {tmp_path}")

        return raw_dataset_mapping

    def merge_mappings(self, dataset_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Merges the provided datasets provided and returns the merged result.

        Args:
        ---
            dataset_mapping (Dict[str, xr.Dataset]):    The dataset mappings 
                                                        to merge.

        Returns:
        ---
            xr.Dataset: The merged dataset.
        -------------------------------------------------------------------"""
        merged_dataset = xr.Dataset()
        for ds in dataset_mapping.values():
            merged_dataset = merged_dataset.merge(ds)
        return merged_dataset

    def apply_corrections(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Pipeline hook that can be used to apply standard corrections for the 
        instrument/measurement or calibrations. This method is called
        immediately after the dataset is converted to standard format and
        before any QC tests are applied.

        If corrections are applied, then the `corrections_applied` attribute
        should be updated on the variable(s) that this method applies
        corrections to.

        Args:
        ---
            dataset (xr.Dataset):   A standardized xarray dataset where the 
                                    variable names correspond with the output 
                                    variable names from the config file.
            raw_mapping (Dict[str, xr.Dataset]):    The raw dataset mapping.
        Returns:
        ---
            xr.Dataset: The input xarray dataset with corrections applied.
        -------------------------------------------------------------------"""
        return dataset
    
    def customize_dataset(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Hook to allow for user customizations to the standardized dataset such
        as inserting a derived variable based on other variables in the
        dataset.  This method is called immediately after the apply_corrections
        hook and before any QC tests are applied.

        Args:
        ---
            dataset (xr.Dataset): The dataset to customize.
            raw_mapping (Dict[str, xr.Dataset]):    The raw dataset mapping.

        Returns:
        ---
            xr.Dataset: The customized dataset.
        -------------------------------------------------------------------"""
        return dataset

    def customize_raw_datasets(self, raw_dataset_mapping: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """-------------------------------------------------------------------
        Hook to allow for user customizations to one or more raw xarray Datasets
        before they merged and used to create the standardized dataset.  The
        raw_dataset_mapping will contain one entry for each file being used
        as input to the pipeline.  The keys are the standardized raw file name,
        and the values are the datasets.

        This method would typically only be used if the user is combining
        multiple files into a single dataset.  In this case, this method may
        be used to correct coordinates if they don't match for all the files,
        or to change variable (column) names if two files have the same
        name for a variable, but they are two distinct variables.

        This method can also be used to check for unique conditions in the raw
        data that should cause a pipeline failure if they are not met.

        This method is called before the inputs are merged and converted to
        standard format as specified by the config file.

        Args:
        ---
            raw_dataset_mapping (Dict[str, xr.Dataset])     The raw datasets to
                                                            customize.

        Returns:
        ---
            Dict[str, xr.Dataset]: The customized raw dataset.
        -------------------------------------------------------------------"""
        return raw_dataset_mapping

    def create_and_persist_plots(self, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Hook to allow users to create plots from the xarray dataset after 
        processing and QC have been applied and just before the dataset is
        saved to disk.

        To save on filesystem space (which is limited when running on the
        cloud via a lambda function), this method should only
        write one plot to local storage at a time. An example of how this 
        could be done is below:

        ```
        filename = DSUtil.get_plot_filename(dataset, "sea_level", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            fig, ax = plt.subplots(figsize=(10,5))
            ax.plot(dataset["time"].data, dataset["sea_level"].data)
            fig.save(tmp_path)
            storage.save(tmp_path)
        
        filename = DSUtil.get_plot_filename(dataset, "qc_sea_level", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            fig, ax = plt.subplots(figsize=(10,5))
            DSUtil.plot_qc(dataset, "sea_level", tmp_path)
            storage.save(tmp_path)
        ```

        Args:
        ---
            dataset (xr.Dataset):   The xarray dataset with customizations and 
                                    QC applied. 
        -------------------------------------------------------------------"""
        pass
        
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
