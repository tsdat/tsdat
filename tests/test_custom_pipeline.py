import os
import shutil
import unittest

import act
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from tsdat.config import Config
from tsdat.io import FilesystemStorage
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil
from tsdat.qc import QC
from tsdat.io import FileHandler

class CustomIngestPipeline(IngestPipeline):
    """-------------------------------------------------------------------
    This is an example class that extends the default IngestPipeline in
    order to hook in custom behavior such as creating custom plots.
    If users need to apply custom changes to the dataset, instrument
    corrections, or create custom plots, they should follow this example
    to extend the IngestPipeline class.
    -------------------------------------------------------------------"""
    def run(self, filepath: str) -> None:
        """-------------------------------------------------------------------
        Runs the Ingest Pipeline from start to finish.

        Args:
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
            dataset = self.apply_corrections(dataset)
            dataset = self.customize_dataset(dataset)

            if self.config.dataset_definition.get_attr('data_level').startswith('b'):
                # If there is previous data in Storage, we need
                # to load up the last file so we can perform
                # continuity checks such as monontonically increasing
                previous_dataset = self.get_previous_dataset(dataset)
                QC.apply_tests(dataset, self.config, previous_dataset)

            # Save the final datastream data to storage
            dataset = self.store_and_reopen_dataset(dataset)
            # self.store_and_reopen_dataset(dataset)

            # Hook to generate plots
            # Users should save plots with self.storage.save(paths_to_plots)
            self.create_and_persist_plots(dataset)

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
            raw_dataset_mapping (Dict[str, xr.Dataset])     The raw datasets to
                                                            customize.

        Returns:
            Dict[str, xr.Dataset]: The customized raw dataset.
        -------------------------------------------------------------------"""
        # In this hook we rename one variable from the surfacetemp file to
        # prevent a naming conflict with a variable in the conductivity file.
        for filename, dataset in raw_dataset_mapping.items():
            if "surfacetemp" in filename: 
                old_name = "Surface Temperature (C)"
                new_name = "surfacetemp - Surface Temperature (C)"
                raw_dataset_mapping[filename] = dataset.rename_vars({old_name: new_name})

        # No customization to raw data - return original dataset
        return raw_dataset_mapping

    def apply_corrections(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Pipeline hook that can be used to apply standard corrections for the
        instrument/measurement or calibrations. This method is called
        immediately after the dataset is converted to standard format and
        before any QC tests are applied.

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
        # No corrections - return the original dataset
        return dataset

    def customize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Hook to allow for user customizations to the standardized dataset such
        as inserting a derived variable based on other variables in the
        dataset.  This method is called immediately after the apply_corrections
        hook and before any QC tests are applied.

        Args:
            dataset (xr.Dataset): The dataset to customize.

        Returns:
            xr.Dataset: The customized dataset.
        -------------------------------------------------------------------"""
        # No customizations - return the original dataset
        return dataset

    def store_and_reopen_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Writes the dataset to a local netcdf file and then uses the 
        DatastreamStorage object to persist it. 

        Args:
            dataset (xr.Dataset): The dataset to store.
        -------------------------------------------------------------------"""
        reopened_dataset = None
        with self.storage.tmp.get_temp_filepath(DSUtil.get_dataset_filename(dataset)) as tmp_path:
            FileHandler.write(dataset, tmp_path)
            self.storage.save(tmp_path)
            reopened_dataset = FileHandler.read(tmp_path)
        return reopened_dataset
    
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
            dataset (xr.Dataset):   The xarray dataset with customizations and
                                    QC applied.
        -------------------------------------------------------------------"""
        for variable_name in dataset.data_vars.keys():
            if variable_name.startswith("qc_") or "time" not in dataset[variable_name].dims:
                continue
            
            filename = DSUtil.get_plot_filename(dataset, variable_name, "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                display = act.plotting.TimeSeriesDisplay(dataset, subplot_shape=(2,), figsize=(15,9), sharex=True)
                display.plot(variable_name, subplot_index=(0,))
                display.qc_flag_block_plot(variable_name, subplot_index=(1,))
                display.fig.savefig(tmp_path)
                plt.close()

                self.storage.save(tmp_path)
        return


class TestCustomIngestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        testdir = os.path.abspath(os.path.dirname(__file__))
        self.basedir = os.path.join(testdir, 'data/custom')

        # Root folder of datastream storage
        self.root = os.path.join(testdir, 'data/storage/root')
        os.makedirs(self.root, exist_ok=True)

        # Input directory where incoming raw files will be dropped
        self.raw = os.path.join(testdir, 'data/storage/raw')
        os.makedirs(self.raw, exist_ok=True)

    def tearDown(self) -> None:
        super().tearDown()

        # Clean up temporary folders
        shutil.rmtree(self.root)
        shutil.rmtree(self.raw)

    def get_raw_file(self, raw_filename):
        """-----------------------------------------------------------------------
        Copies the raw file into the temporary raw folder representing the pipeline
        input folder.  We need to do this because the pipeline will remove the
        processed file from the input folder if it completes with no error.
        -----------------------------------------------------------------------"""
        original_raw_file = os.path.join(self.basedir, raw_filename)
        temp_raw_file = os.path.join(self.raw, raw_filename)
        shutil.copy(original_raw_file, temp_raw_file)
        return temp_raw_file

    def test_custom_ingest(self):
        raw_file = self.get_raw_file('buoy.z05.00.20201117.000000.zip')
        # raw_file = self.get_raw_file('buoy.z05.00.20201004.000000.zip')
        # raw_file = self.get_raw_file('buoy.z05.00.20201004.000000_no_gill_waves.zip')
        config_file = os.path.join(self.basedir, 'custom_ingest.yml')

        storage: FilesystemStorage = FilesystemStorage(self.root)
        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = CustomIngestPipeline(config, storage)
        pipeline.run(raw_file)

    def test_waves_ingest(self):
        raw_file = self.get_raw_file('buoy.z05.00.20201117.000000.waves.csv')
        # raw_file = self.get_raw_file('buoy.z05.00.20201004.000000.zip')
        # raw_file = self.get_raw_file('buoy.z05.00.20201004.000000_no_gill_waves.zip')
        config_file = os.path.join(self.basedir, 'waves_ingest.yml')

        storage: FilesystemStorage = FilesystemStorage(self.root)
        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = CustomIngestPipeline(config, storage)
        pipeline.run(raw_file)

if __name__ == '__main__':
    unittest.main()
