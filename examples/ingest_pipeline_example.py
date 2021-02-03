import os
import shutil
from typing import Dict

import xarray as xr
from tsdat.config import Config
from tsdat.io import FilesystemStorage
from tsdat.pipeline import IngestPipeline


"""-----------------------------------------------------------------------
Step 1:  Create custom pipeline class
-----------------------------------------------------------------------"""
class CustomIngestPipeline(IngestPipeline):
    """-------------------------------------------------------------------
    This is an example class that extends the default IngestPipeline in
    order to hook in custom behavior such as creating custom plots.
    If users need to apply custom changes to the dataset, instrument
    corrections, or create custom plots, they should follow this example
    to extend the IngestPipeline class.
    -------------------------------------------------------------------"""

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
        pass


# Set up references to folders that will be used in this example:
examples_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(examples_dir, 'data')
templates_dir = os.path.join(examples_dir, 'templates')

# Root folder of datastream storage
root_dir = os.path.join(data_dir, 'storage/root')
os.makedirs(root_dir, exist_ok=True)

# Input directory where incoming raw files will be dropped
input_dir = os.path.join(data_dir, 'storage/input')
os.makedirs(input_dir, exist_ok=True)


def get_input_file(raw_filename):
    """-----------------------------------------------------------------------
    Copies a raw file into the pipeline input folder.  We need to do this
    because the pipeline will remove the processed file from the input folder
    if it completes with no error.
    -----------------------------------------------------------------------"""
    original_raw_file = os.path.join(data_dir, raw_filename)
    input_file = os.path.join(input_dir, raw_filename)
    shutil.copy(original_raw_file, input_file)
    return input_file


"""-----------------------------------------------------------------------
Step 2:  Run the customzied pipeline defined above
-----------------------------------------------------------------------"""
# Get the input file that will be processed:
input_file = get_input_file('buoy.z05.00.20201004.000000_no_gill_waves.zip')

# Get the config file that will be used to define the standardized dataset:
config_file = os.path.join(templates_dir, 'ingest_pipeline_example.yml')

# Create the storage class. (We are writing to local filesystem, so we use
# FilesystemStorage.  If we were running this on the cloud, we would use
# AWSStorage.)
storage: FilesystemStorage = FilesystemStorage(root_dir)

# Load the config file
config: Config = Config.load(config_file)

# Create our custom pipeline class:
pipeline: CustomIngestPipeline = CustomIngestPipeline(config, storage)

# Run the pipeline on the input file
pipeline.run(input_file)