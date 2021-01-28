import zipfile
import tarfile
import xarray as xr
from typing import Dict, List
from .pipeline import Pipeline
from tsdat.io.filehandlers import FileHandler
from tsdat.utils import DSUtil
from tsdat.qc import QC


class IngestPipeline(Pipeline):       

    def run(self, filepath: str) -> None:
        """-------------------------------------------------------------------
        Runs the Ingest Pipeline from start to finish.

        Args:
            filepath (str): The path to the file (or archive containing
                            a collection of files) to run the Ingest Pipeline 
                            on.
        -------------------------------------------------------------------"""
        # If the file is a zip/tar, then we need to extract the individual files
        # file_paths = self.extract_raw_files(filepath)
        with self.storage._tmp.extract_raw_files(filepath) as file_paths:

            # Open each raw file into a Dataset, standardize the raw file names and store.
            # Use storage and FileHandler to access and read the file.
            # Involves opening the raw
            # files to get the timestamp of the first point of data since this is
            # required/recommended by raw file naming conventions.
            raw_dataset_mapping: Dict[str, xr.Dataset] = self.read_and_persist_raw_files(file_paths)

            raw_dataset = self.merge_mappings(raw_dataset_mapping)

            # Process the data
            dataset = self.standardize_dataset(raw_dataset, raw_dataset_mapping)
            dataset = self.apply_corrections(dataset)
            dataset = self.customize_dataset(dataset)

            if self.config.dataset_definition.data_level.startswith('b'):
                # If there is previous data in Storage, we need
                # to load up the last file so we can perform
                # continuity checks such as monontonically increasing
                previous_dataset = self.get_previous_dataset(dataset)
                QC.apply_tests(dataset, self.config, previous_dataset)

            # Hook to generate plots
            # Users should save plots with self.storage.save(paths_to_plots)
            self.create_and_persist_plots(dataset)

            # Save the final datastream data to storage
            self.store_dataset(dataset)

    def extract_raw_files(self, filepath: str) -> List[str]:
        """-------------------------------------------------------------------
        If provided a path to an archive file, this function will unzip the
        archive and return a list of complete paths to each file.
        Zip or tar/targz archive files are supported.

        Args:
            filepath (str): A path to an archive file or a regular file.

        Returns:
            List[str]:  A list of complete paths to the unzipped files or 
                        `[filepath]` if `filepath` is not a .zip file.
        -------------------------------------------------------------------"""
        extracted_files = [filepath]

        if tarfile.is_tarfile(filepath) or zipfile.is_zipfile(filepath):
            # TODO: make sure that storage creates a unique folder name to unzip to
            extracted_files = self.storage._tmp.unzip(filepath)

        return extracted_files

        # if not filepath.endswith(".zip"):
        #     if os.path.isfile(filepath):
        #         return [filepath]
        #     raise ValueError("filepath must be a .zip archive or a file")
        #
        # # If target_dir not provided, make it be the parent directory of the
        # # filepath provided
        # if not target_dir:
        #     target_dir, _ = os.path.split(filepath)
        # # Extract into a temporary folder in the target_dir
        # temp_dir = f"{target_dir}/.unzipped"
        # os.makedirs(temp_dir, exist_ok=False)
        # with zipfile.ZipFile(filepath, 'r') as zipped:
        #     zipped.extractall(temp_dir)
        # # Move files from temp_dir into target_dir and remove temp_dir
        # filenames = os.listdir(temp_dir)
        # temp_paths = [os.path.join(temp_dir,   file) for file in filenames]
        # new_paths  = [os.path.join(target_dir, file) for file in filenames]
        # for temp_path, new_path in zip(temp_paths, new_paths):
        #     shutil.move(temp_path, new_path)
        # os.rmdir(temp_dir)
        # return new_paths
    
    def read_and_persist_raw_files(self, file_paths: List[str]) -> List[str]:
        """-------------------------------------------------------------------
        Renames the provided RAW files according to MHKiT-Cloud Data Standards 
        naming conventions for RAW data and returns a list of filepaths to the 
        renamed files.

        Args:
            file_paths (List[str]): A list of paths to the original raw files.

        Returns:
            List[str]: A list of paths to the renamed raw files.
        -------------------------------------------------------------------"""
        raw_dataset_mapping = {}

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        for file_path in file_paths:

            # read the raw file into a dataset
            with self.storage._tmp.fetch(file_path) as tmp_path:
                dataset = FileHandler.read(tmp_path)

                # create the standardized name for raw file
                new_filename = DSUtil.get_raw_filename(dataset, tmp_path, self.config)

                # add the raw dataset to our dictionary
                raw_dataset_mapping[new_filename] = dataset

                # save the raw data to storage
                self.storage.save(tmp_path, new_filename)

        return raw_dataset_mapping

    def merge_mappings(self, dataset_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Merges the provided datasets provided and returns the merged result.

        Args:
            dataset_mapping (Dict[str, xr.Dataset]):    The dataset mappings 
                                                        to merge.

        Returns:
            xr.Dataset: The merged dataset.
        -------------------------------------------------------------------"""
        merged_dataset = xr.Dataset()
        for ds in dataset_mapping.values:
            merged_dataset.merge(ds, inplace=True)
        return merged_dataset

    def apply_corrections(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Pipeline hook that can be used to apply standard corrections for the 
        instrument/measurement or calibrations. It can also be used to insert 
        any derived properties into the dataset. This method is called 
        immediately after the dataset is converted to standard format. 

        If corrections are applied, then the `corrections_applied` attribute
        should be updated on the variable(s) that this method applies
        corrections to.

        Args:
            dataset (xr.Dataset):   A standardized xarray dataset where the 
                                    variable names correspond with the output 
                                    variable names from the config file.
        Returns:
            xr.Dataset: The input xarray dataset with corrections applied.
        -------------------------------------------------------------------"""
        return dataset
    
    def customize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Hook to allow for user customizations to the xarray Dataset before it 
        is validated and saved to the DatastreamStorage.

        Args:
            dataset (xr.Dataset): The dataset to customize.

        Returns:
            xr.Dataset: The customized dataset.
        -------------------------------------------------------------------"""
        return dataset

    def create_and_persist_plots(self, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Hook to allow users to create plots from the xarray dataset after 
        processing and QC has been applied and just before the dataset is 
        saved to disk. To save on filesystem space, this method should only 
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
            dataset (xr.Dataset):   The xarray dataset with customizations and 
                                    QC applied. 
        -------------------------------------------------------------------"""
        pass
        
    def get_previous_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Utility method to retrieve the previous set of data for the same 
        datastream as the provided dataset from the DatastreamStorage.

        Args:
            dataset (xr.Dataset):   The reference dataset that will be used to
                                    search the DatastreamStore for prior data.

        Returns:
            xr.Dataset: The previous dataset from the DatastreamStorage if it
                        exists, else None.
        -------------------------------------------------------------------"""
        dataset = None
        start_date, start_time = DSUtil.get_start_time(dataset)
        datastream_name = DSUtil.get_datastream_name(dataset, self.config)

        with self.storage._tmp.fetch_previous_file(datastream_name, start_date, start_time) as netcdf_file:

            dataset = None
            if netcdf_file:
                dataset = FileHandler.read(netcdf_file, config=self.config)

        return dataset
    
    def store_dataset(self, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Writes the dataset to a local netcdf file and then uses the 
        DatastreamStorage object to persist it. 

        Args:
            dataset (xr.Dataset): The dataset to store.
        -------------------------------------------------------------------"""
        with self.storage._tmp.get_temp_filepath(DSUtil.get_dataset_filename(dataset)) as tmp_path:
            dataset.to_netcdf(tmp_path)
            self.storage.save(tmp_path)
