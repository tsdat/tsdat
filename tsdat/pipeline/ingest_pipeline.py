import warnings
import xarray as xr
from typing import Dict, List, Union
from tsdat.qc import QualityManagement
from tsdat.utils import DSUtil
from .pipeline import Pipeline


class IngestPipeline(Pipeline):
    """The IngestPipeline class is designed to read in raw, non-standardized
    data and convert it to a standardized format by embedding metadata,
    applying quality checks and quality controls, and by saving the
    now-processed data in a standard file format."""

    def run(self, filepath: Union[str, List[str]]) -> xr.Dataset:
        """Runs the IngestPipeline from start to finish.

        :param filepath:
            The path(s) to the file(s) to run the pipeline on.
        :type filepath: Union[str, List[str]]
        """
        # Open each raw file into a Dataset, standardize the raw file names and store.
        raw_dataset_mapping: Dict[str, xr.Dataset] = self.read_and_persist_raw_files(
            filepath
        )

        # Customize the raw data before it is used as input for standardization
        raw_dataset_mapping: Dict[str, xr.Dataset] = self.hook_customize_raw_datasets(
            raw_dataset_mapping
        )

        # Standardize the dataset and apply corrections / customizations
        dataset = self.standardize_dataset(raw_dataset_mapping)
        dataset = self.hook_customize_dataset(dataset, raw_dataset_mapping)

        # Apply quality control / quality assurance to the dataset.
        previous_dataset = self.get_previous_dataset(dataset)
        dataset = QualityManagement.run(dataset, self.config, previous_dataset)

        # Apply any final touches to the dataset and persist the dataset
        dataset = self.hook_finalize_dataset(dataset)
        dataset = self.decode_cf(dataset)
        self.storage.save(dataset)

        # Hook to generate custom plots
        self.hook_generate_and_persist_plots(dataset)

        return dataset

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        """Hook to allow for user customizations to the standardized dataset
        such as inserting a derived variable based on other variables in the
        dataset. This method is called immediately after the
        ``standardize_dataset`` method and before ``QualityManagement`` has
        been run.

        :param dataset: The dataset to customize.
        :type dataset: xr.Dataset
        :param raw_mapping: The raw dataset mapping.
        :type raw_mapping: Dict[str, xr.Dataset]
        :return: The customized dataset.
        :rtype: xr.Dataset
        """
        return dataset

    def hook_customize_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:
        """Hook to allow for user customizations to one or more raw xarray
        Datasets before they merged and used to create the standardized
        dataset. The raw_dataset_mapping will contain one entry for each file
        being used as input to the pipeline.  The keys are the standardized
        raw file name, and the values are the datasets.

        This method would typically only be used if the user is combining
        multiple files into a single dataset.  In this case, this method may
        be used to correct coordinates if they don't match for all the files,
        or to change variable (column) names if two files have the same
        name for a variable, but they are two distinct variables.

        This method can also be used to check for unique conditions in the raw
        data that should cause a pipeline failure if they are not met.

        This method is called before the inputs are merged and converted to
        standard format as specified by the config file.

        :param raw_dataset_mapping: The raw datasets to customize.
        :type raw_dataset_mapping: Dict[str, xr.Dataset]
        :return: The customized raw datasets.
        :rtype: Dict[str, xr.Dataset]
        """
        return raw_dataset_mapping

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """Hook to apply any final customizations to the dataset before it is
        saved. This hook is called after QualityManagement has been run and
        immediately before the dataset it saved to file.

        :param dataset: The dataset to finalize.
        :type dataset: xr.Dataset
        :return: The finalized dataset to save.
        :rtype: xr.Dataset
        """
        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset) -> None:
        """Hook to allow users to create plots from the xarray dataset after
        the dataset has been finalized and just before the dataset is
        saved to disk.

        To save on filesystem space (which is limited when running on the
        cloud via a lambda function), this method should only
        write one plot to local storage at a time. An example of how this
        could be done is below:

        .. code-block:: python

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

        :param dataset:
            The xarray dataset with customizations and QualityManagement
            applied.
        :type dataset: xr.Dataset
        """
        pass

    def read_and_persist_raw_files(
        self, filepaths: Union[str, List[str]]
    ) -> Dict[str, xr.Dataset]:
        """------------------------------------------------------------------------------------
        Renames the provided raw files according to our naming conventions and returns a
        mapping of the renamed filepaths to raw `xr.Dataset` objects.

        Args:
            file_paths (List[str]): The path(s) to the raw file(s).

        Returns:
            Dict[str, xr.Dataset]: The mapping of raw filepaths to raw xr.Dataset objects.

        ------------------------------------------------------------------------------------"""
        raw_mapping: Dict[str, xr.Dataset] = dict()

        if isinstance(filepaths, str):
            filepaths = [filepaths]

        for filepath in filepaths:

            extracted = self.storage.handlers.read(file=filepath, name=filepath)
            if not extracted:
                warnings.warn(f"Couldn't use extracted raw file: {filepath}")
                continue

            new_filename = DSUtil.get_raw_filename(extracted, filepath, self.config)
            self.storage.save(filepath, new_filename=new_filename)

            if isinstance(extracted, xr.Dataset):
                extracted = {new_filename: extracted}

            raw_mapping.update(extracted)

        return raw_mapping
