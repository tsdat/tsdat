import re
import warnings
import xarray as xr
from typing import Dict, List, Union
from .pipeline import Pipeline
from tsdat.io.filehandlers import FileHandler
from tsdat.qc import QC
from tsdat.utils import DSUtil
from tsdat.config import DatasetDefinition, VariableDefinition
from .pipeline import Pipeline


class IngestPipeline(Pipeline):

    def run(self, filepath: Union[str, List[str]]) -> None:
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
            raw_dataset_mapping: Dict[str, xr.Dataset] = self.read_and_persist_raw_files(file_paths)

            # Customize the raw data before it is used as input for standardization
            raw_dataset_mapping: Dict[str, xr.Dataset] = self.hook_customize_raw_datasets(raw_dataset_mapping)

            # Standardize the dataset and apply corrections / customizations
            dataset = self.standardize_dataset(raw_dataset_mapping)
            dataset = self.hook_apply_corrections(dataset, raw_dataset_mapping)
            dataset = self.hook_customize_dataset(dataset, raw_dataset_mapping)

            # Apply quality control / quality assurance to the dataset.
            previous_dataset = self.get_previous_dataset(dataset)
            dataset = QC.apply_tests(dataset, self.config, previous_dataset)

            # Apply any final touches to the dataset and persist the dataset
            dataset = self.hook_finalize_dataset(dataset)
            dataset = self.store_and_reopen_dataset(dataset)

            # Hook to generate custom plots
            self.hook_generate_and_persist_plots(dataset)

        # Make sure that any temp files are cleaned up
        self.storage.tmp.clean()

    def hook_apply_corrections(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
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
    
    def hook_customize_dataset(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
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

    def hook_customize_raw_datasets(self, raw_dataset_mapping: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
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

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Hook to apply any final customizations to the dataset before it is
        saved. This hook is called after quality tests have been applied.

        Args:
            dataset (xr.Dataset): The dataset to finalize.

        Returns:
            xr.Dataset: The finalized dataset to save.
        -------------------------------------------------------------------"""
        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset) -> None:
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

    def reduce_raw_datasets(self, raw_mapping: Dict[str, xr.Dataset], definition: DatasetDefinition) -> List[xr.Dataset]:
        """-------------------------------------------------------------------
        Removes unused variables from each raw dataset in the raw mapping and
        performs input to output naming and unit conversions as defined in the
        dataset definition.

        Args:
        ---
            raw_mapping (Dict[str, xr.Dataset]):    The raw xarray dataset mapping.
            definition (DatasetDefinition): The DatasetDefinition used to
                                            select the variables to keep.

        Returns:
        ---
            List[xr.Dataset]:   A list of reduced datasets.
        -------------------------------------------------------------------"""

        def _find_files_with_variable(variable: VariableDefinition) -> List[xr.Dataset]:
            files = []
            variable_name = variable.get_input_name()
            for filename, dataset in raw_mapping.items():
                if variable_name in dataset.variables:
                    files.append(filename)
            return files

        def _find_files_with_regex(variable: VariableDefinition) -> List[xr.Dataset]:
            regex = re.compile(variable.input.file_pattern)
            return list(filter(regex.search, raw_mapping.keys()))

        # Determine which datasets will be used to retrieve variables
        retrieval_rules: Dict[str, List[VariableDefinition]] = {}
        for variable in definition.vars.values():

            if variable.has_input():
                search_func = _find_files_with_variable

                if hasattr(variable.input, "file_pattern"):
                    search_func = _find_files_with_regex

                filenames = search_func(variable)
                for filename in filenames:
                    file_rules = retrieval_rules.get(filename, [])
                    retrieval_rules[filename] = file_rules + [variable]

        # Build the list of reduced datasets
        reduced_datasets: List[xr.Dataset] = []
        for filename, variable_definitions in retrieval_rules.items():
            raw_dataset = raw_mapping[filename]
            reduced_dataset = self.reduce_raw_dataset(raw_dataset, variable_definitions, definition)
            reduced_datasets.append(reduced_dataset)

        return reduced_datasets

    def reduce_raw_dataset(self, raw_dataset: xr.Dataset, variable_definitions: List[VariableDefinition], definition: DatasetDefinition) -> xr.Dataset:
        """-------------------------------------------------------------------
        Removes unused variables from the raw dataset provided and keeps only
        the variables and coordinates pertaining to the provided variable
        definitions. Also performs input to output naming and unit conversions
        as defined in the dataset definition.

        Args:
        ---
            raw_mapping (Dict[str, xr.Dataset]):    The raw xarray dataset mapping.
            variable_definitions (List[VariableDefinition]):    List of variables to keep.
            definition (DatasetDefinition): The DatasetDefinition used to select the variables to keep.
        
        Returns:
        ---
            xr.Dataset: The reduced dataset.
        -------------------------------------------------------------------"""
        def _retrieve_and_convert(variable: VariableDefinition) -> Dict:
            var_name = variable.get_input_name()
            if var_name in raw_dataset.variables:
                data_array = raw_dataset[var_name]

                # Input to output unit conversion
                data = data_array.values
                in_units = variable.get_input_units()
                out_units = variable.get_output_units()
                data = variable.input.converter.run(data, in_units, out_units)

                # Consolidate retrieved data
                dictionary = {
                    "attrs":    data_array.attrs,
                    "dims":     list(variable.dims.keys()),
                    "data":     data
                }
                return dictionary
            return None

        # Get the coordinate definitions of the given variables
        dims: List[str] = []
        for var_definition in variable_definitions:
            dims.extend(var_definition.get_coordinate_names())
        dims: List[str] = list(dict.fromkeys(dims))
        coord_definitions = [definition.get_variable(coord_name) for coord_name in dims]

        coords = {}
        for coordinate in coord_definitions:
            coords[coordinate.name] = _retrieve_and_convert(coordinate)

        data_vars = {}
        for variable in variable_definitions:
            data_var = _retrieve_and_convert(variable)
            if data_var:
                data_vars[variable.name] = data_var

        reduced_dict = {
            "attrs":        raw_dataset.attrs,
            "dims":         dims,
            "coords":       coords,
            "data_vars":    data_vars
        }
        return xr.Dataset.from_dict(reduced_dict)
