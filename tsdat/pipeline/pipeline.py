import re
import abc
import datetime
import numpy as np
import xarray as xr
from typing import Any, Dict, List, Union

from tsdat.utils import DSUtil
from tsdat.config import Config, DatasetDefinition, VariableDefinition
from tsdat.io import DatastreamStorage, FileHandler


class Pipeline(abc.ABC):
    """This class serves as the base class for all tsdat data pipelines.

    :param pipeline_config:
        The pipeline config file. Can be either a config object, or the
        path to the pipeline config file that should be used with this
        pipeline.
    :type pipeline_config: Union[str, Config]
    :param storage_config:
        The storage config file. Can be either a config object, or the
        path to the storage config file that should be used with this
        pipeline.
    :type storage_config: Union[str, DatastreamStorage]
    """

    def __init__(
        self,
        pipeline_config: Union[str, Config],
        storage_config: Union[str, DatastreamStorage],
    ) -> None:
        # We can pass either a Config object or the path to a config file
        config = pipeline_config
        if isinstance(pipeline_config, str):
            config = Config.load(pipeline_config)
        self.config = config

        # We can pass either a DatastreamStorage object or the path to a config file
        storage = storage_config
        if isinstance(storage_config, str):
            storage = DatastreamStorage.from_config(storage_config)
        self.storage = storage

    @abc.abstractmethod
    def run(self, filepath: Union[str, List[str]]):
        """This method is the entry point for the pipeline. It will take one
        or more file paths and process them from start to finish. All classes
        extending the Pipeline class must implement this method.

        :param filepath:
            The path or list of paths to the file(s) to run the pipeline on.
        :type filepath: Union[str, List[str]]
        """
        return

    def standardize_dataset(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """Standardizes the dataset by applying variable name and units
        conversions as defined by the pipeline config file. This method
        returns the standardized dataset.

        :param raw_mapping: The raw dataset mapping.
        :type raw_mapping: Dict[str, xr.Dataset]
        :return: The standardized dataset.
        :rtype: xr.Dataset
        """
        definition = self.config.dataset_definition

        raw_datasets = self.reduce_raw_datasets(raw_mapping, definition)

        # Merge only preserves global attributes from the first dataset.
        # Variable attributes are only preserved from the first dataset.
        merged_dataset = xr.merge(raw_datasets)

        # Check required variables are in merged dataset
        self.check_required_variables(merged_dataset, definition)

        # Ensure all variables are initialized. Computed variables, variables
        # that are set statically, and variables that weren't retrieved should
        # be initialized.
        merged_dataset = self.add_static_variables(merged_dataset, definition)
        merged_dataset = self.add_missing_variables(merged_dataset, definition)

        # Add global and variable attributes to dataset
        merged_dataset = self.add_attrs(merged_dataset, raw_mapping, definition)

        return merged_dataset

    def check_required_variables(self, dataset: xr.Dataset, dod: DatasetDefinition):
        """Function to throw an error if a required variable could not be
        retrieved.

        :param dataset: The dataset to check.
        :type dataset: xr.Dataset
        :param dod: The DatasetDefinition used to specify required variables.
        :type dod: DatasetDefinition
        :raises Exception:
            Raises an exception to indicate the variable could not be
            retrieved.
        """
        for variable in dod.coords.values():
            if variable.is_required() and variable.name not in dataset.variables:
                raise Exception(
                    f"Required coordinate variable '{variable.name}' could not be retrieved."
                )
        for variable in dod.vars.values():
            if variable.is_required() and variable.name not in dataset.variables:
                raise Exception(
                    f"Required variable '{variable.name}' could not be retrieved."
                )

    def add_static_variables(
        self, dataset: xr.Dataset, dod: DatasetDefinition
    ) -> xr.Dataset:
        """Uses the DatasetDefinition to add static variables (variables whose
        data are defined in the pipeline config file) to the output dataset.

        :param dataset: The dataset to add static variables to.
        :type dataset: xr.Dataset
        :param dod: The DatasetDefinition to pull data from.
        :type dod: DatasetDefinition
        :return: The original dataset with added variables from the config
        :rtype: xr.Dataset
        """
        coords, data_vars = {}, {}
        for variable in dod.get_static_variables():
            if variable.is_coordinate():
                coords[variable.name] = variable.to_dict()
            else:
                data_vars[variable.name] = variable.to_dict()
        static_ds = xr.Dataset.from_dict({"coords": coords, "data_vars": data_vars})
        return xr.merge([dataset, static_ds])

    def add_missing_variables(
        self, dataset: xr.Dataset, dod: DatasetDefinition
    ) -> xr.Dataset:
        """Uses the dataset definition to initialize variables that are
        defined in the dataset definiton but did not have input. Uses the
        appropriate shape and _FillValue to initialize each variable.

        :param dataset: The dataset to add the variables to.
        :type dataset: xr.Dataset
        :param dod: The DatasetDefinition to use.
        :type dod: DatasetDefinition
        :return: The original dataset with variables that still need to be initialized, initialized.
        :rtype: xr.Dataset
        """
        coords, data_vars = {}, {}
        for var_name, var_def in dod.vars.items():
            if var_name not in dataset.variables:
                if var_def.is_coordinate():
                    coords[var_name] = var_def.to_dict()
                    shape = dod.dimensions[var_name].length
                    coords[var_name]["data"] = np.full(shape, var_def.get_FillValue())
                else:
                    data_vars[var_name] = var_def.to_dict()
                    shape = [len(dataset[dim_name]) for dim_name in var_def.dims.keys()]
                    data_vars[var_name]["data"] = np.full(
                        shape, var_def.get_FillValue()
                    )

        missing_vars_ds = xr.Dataset.from_dict(
            {"coords": coords, "data_vars": data_vars}
        )
        return xr.merge([dataset, missing_vars_ds])

    def add_attrs(
        self,
        dataset: xr.Dataset,
        raw_mapping: Dict[str, xr.Dataset],
        dod: DatasetDefinition,
    ) -> xr.Dataset:
        """Adds global and variable-level attributes to the dataset from the
        DatasetDefinition object.

        :param dataset: The dataset to add attributes to.
        :type dataset: xr.Dataset
        :param raw_mapping:
            The raw dataset mapping. Used to set the ``input_files`` global
            attribute.
        :type raw_mapping: Dict[str, xr.Dataset]
        :param dod: The DatasetDefinition containing the attributes to add.
        :type dod: DatasetDefinition
        :return: The original dataset with the attributes added.
        :rtype: xr.Dataset
        """

        def _set_attr(obj: Any, att_name: str, att_val: Any):
            if hasattr(obj, "attrs"):
                if hasattr(obj.attrs, att_name):
                    prev_val = obj.attrs[att_name]
                    UserWarning(
                        f"Warning: Overriding attribute {att_name}. Previously was '{prev_val}'"
                    )
                obj.attrs[att_name] = att_val
            else:
                UserWarning(f"Warning: Object {str(obj)} has no 'attrs' attribute.")

        for att_name, att_value in dod.attrs.items():
            _set_attr(dataset, att_name, att_value)

        for coord, coord_def in dod.coords.items():
            for att_name, att_value in coord_def.attrs.items():
                _set_attr(dataset[coord], att_name, att_value)

        for var, var_def in dod.vars.items():
            for att_name, att_value in var_def.attrs.items():
                _set_attr(dataset[var], att_name, att_value)

        dataset.attrs["input_files"] = list(raw_mapping.keys())

        history = f"Ran at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.attrs["history"] = history

        return dataset

    def get_previous_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """Utility method to retrieve the previous set of data for hte same
        datastream as the provided dataset from the DatastreamStorage.

        :param dataset:
            The reference dataset that will be used to search the
            DatastreamStore for prior data.
        :type dataset: xr.Dataset
        :return:
            The previous dataset from the DatastreamStorage if it exists,
            otherwise None.
        :rtype: xr.Dataset
        """
        prev_dataset = None
        start_date, start_time = DSUtil.get_start_time(dataset)
        datastream_name = DSUtil.get_datastream_name(dataset, self.config)

        with self.storage.tmp.fetch_previous_file(
            datastream_name, f"{start_date}.{start_time}"
        ) as netcdf_file:
            if netcdf_file:
                prev_dataset = FileHandler.read(netcdf_file, config=self.config)

        return prev_dataset

    def reduce_raw_datasets(
        self, raw_mapping: Dict[str, xr.Dataset], definition: DatasetDefinition
    ) -> List[xr.Dataset]:
        """Removes unused variables from each raw dataset in the raw mapping
        and performs input to output naming and unit conversions as defined
        in the dataset definition.

        :param raw_mapping: The raw xarray dataset mapping.
        :type raw_mapping: Dict[str, xr.Dataset]
        :param definition:
            The DatasetDefinition used to select the variables to keep.
        :type definition: DatasetDefinition
        :return: A list of reduced datasets.
        :rtype: List[xr.Dataset]
        """

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
        variables = list(definition.vars.values()) + list(definition.coords.values())
        for variable in variables:

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
            reduced_dataset = self.reduce_raw_dataset(
                raw_dataset, variable_definitions, definition
            )
            reduced_datasets.append(reduced_dataset)

        return reduced_datasets

    def reduce_raw_dataset(
        self,
        raw_dataset: xr.Dataset,
        variable_definitions: List[VariableDefinition],
        definition: DatasetDefinition,
    ) -> xr.Dataset:
        """Removes unused variables from the raw dataset provided and keeps
        only the variables and coordinates pertaining to the provdided
        variable definitions. Also performs input to output naming and unit
        conversions as defined in the DatasetDefinition.

        :param raw_dataset: The raw dataset mapping.
        :type raw_dataset: xr.Dataset
        :param variable_definitions: List of variables to keep.
        :type variable_definitions: List[VariableDefinition]
        :param definition:
            The DatasetDefinition used to select the variables to keep.
        :type definition: DatasetDefinition
        :return: The reduced dataset.
        :rtype: xr.Dataset
        """

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
                    "attrs": data_array.attrs,
                    "dims": list(variable.dims.keys()),
                    "data": data,
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
            "attrs": raw_dataset.attrs,
            "dims": dims,
            "coords": coords,
            "data_vars": data_vars,
        }
        return xr.Dataset.from_dict(reduced_dict)

    def decode_cf(self, dataset: xr.Dataset) -> xr.Dataset:
        """------------------------------------------------------------------------------------
            Decodes the dataset according to CF conventions. This helps ensure that the dataset
            is formatted correctly after it has been constructed from unstandardized sources or
            heavily modified.
            Args:
                dataset (xr.Dataset): The dataset to decode.
            Returns:
                xr.Dataset: The decoded dataset.
            ------------------------------------------------------------------------------------"""
        # We have to make sure that time variables do not have units set as attrs, and
        # instead have units set on the encoding or else xarray will crash when trying
        # to save: https://github.com/pydata/xarray/issues/3739
        for variable in dataset.variables.values():
            if variable.data.dtype.type == np.datetime64 and "units" in variable.attrs:
                units = variable.attrs["units"]
                del variable.attrs["units"]
                variable.encoding["units"] = units

        # Leaving the "dtype" entry in the encoding causes a crash when calling
        # `dataset.to_netcdf`. Related to but not fixed by https://github.com/pydata/xarray/pull/4684
        ds = xr.decode_cf(dataset)
        for variable in ds.variables.values():
            if variable.data.dtype.type == np.datetime64:
                if "dtype" in variable.encoding:
                    del variable.encoding["dtype"]
        return ds
