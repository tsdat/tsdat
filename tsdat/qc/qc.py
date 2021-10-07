import numpy as np
import xarray as xr

from tsdat.config import Config, QualityManagerDefinition
from tsdat.constants import VARS
from tsdat.utils import DSUtil
from tsdat.config.utils import instantiate_handler


class QualityManagement:
    """Class that provides static helper functions for providing quality
    control checks on a tsdat-standardized xarray dataset.
    """

    @staticmethod
    def run(ds: xr.Dataset, config: Config, previous_data: xr.Dataset) -> xr.Dataset:
        """Applies the Quality Managers defined in the given Config to this dataset.
        QC results will be embedded in the dataset.  QC metadata will be
        stored as attributes, and QC flags will be stored as a bitwise integer
        in new companion qc_ variables that are added to the dataset.
        This method will create QC companion variables if they don't exist.

        :param ds: The dataset to apply quality managers to
        :type ds: xr.Dataset
        :param config: A configuration definition (loaded from yaml)
        :type config: Config
        :param previous_data: A dataset from the previous processing interval
            (i.e., file).  This is used to check for consistency between files,
            such as for monitonic or delta checks when we need to check the previous value.
        :type previous_data: xr.Dataset
        :return: The dataset after quality checkers and handlers have been applied.
        :rtype: xr.Dataset
        """
        for definition in config.quality_managers.values():
            quality_manager = QualityManager(ds, config, definition, previous_data)
            ds = quality_manager.run()

        return ds


class QualityManager:
    """Applies a single Quality Manager to the given Dataset, as defined by
    the Config

    :param ds: The dataset for which we will perform quality management.
    :type ds: xr.Dataset
    :param config: The Config from the pipeline definition file.
    :type config: Config
    :param definition: Definition of the quality test this class manages.
    :type definition: QualityManagerDefinition
    :param previous_data: A dataset from the previous processing interval
        (i.e., file).  This is used to check for consistency between files,
        such as for monitonic or delta checks when we need to check the previous value.
    :type previous_data: xr.Dataset
    """

    def __init__(
        self,
        ds: xr.Dataset,
        config: Config,
        definition: QualityManagerDefinition,
        previous_data: xr.Dataset,
    ):

        # Get the variables this quality manager applies to
        variable_names = definition.variables

        # Convert the list to upper case in case the user made a typo in the yaml
        variable_names_upper = [x.upper() for x in variable_names]

        # Add variables where a keyword was used
        if VARS.COORDS in variable_names_upper:
            variable_names.remove(VARS.COORDS)
            variable_names.extend(DSUtil.get_coordinate_variable_names(ds))

        if VARS.DATA_VARS in variable_names_upper:
            variable_names.remove(VARS.DATA_VARS)
            variable_names.extend(DSUtil.get_non_qc_variable_names(ds))

        if VARS.ALL in variable_names_upper:
            variable_names.remove(VARS.ALL)
            variable_names.extend(DSUtil.get_coordinate_variable_names(ds))
            variable_names.extend(DSUtil.get_non_qc_variable_names(ds))

        # Remove any duplicates while preserving insertion order
        variable_names = list(dict.fromkeys(variable_names))

        # Exclude any excludes
        excludes = definition.exclude
        for exclude in excludes:
            variable_names.remove(exclude)

        # Get the quality checker
        quality_checker = instantiate_handler(
            ds, previous_data, definition, handler_desc=definition.checker
        )

        # Get the quality handlers
        handlers = definition.handlers

        self.ds = ds
        self.config = config
        self.variable_names = variable_names
        self.checker = quality_checker
        self.handlers = handlers
        self.definition: QualityManagerDefinition = definition
        self.previous_data = previous_data

    def run(self) -> xr.Dataset:
        """Runs the QualityChecker and QualityHandler(s) for each specified
        variable as defined in the config file.

        :return: The dataset after the quality checker and the quality handlers
            have been run.
        :raises QCError: A QCError indicates that a fatal error has occurred.
        :rtype: xr.Dataset
        """
        for variable_name in self.variable_names:

            # Apply the quality checker
            results_array: np.ndarray = self.checker.run(variable_name)
            if results_array is None:
                results_array = np.zeros_like(self.ds[variable_name].data, dtype="bool")

            # Apply quality handlers
            if self.handlers is not None:
                for handler in self.handlers:
                    quality_handler = instantiate_handler(
                        self.ds,
                        self.previous_data,
                        self.definition,
                        handler_desc=handler,
                    )
                    quality_handler.run(variable_name, results_array)
                    self.ds: xr.Dataset = quality_handler.ds
                self.checker.ds = self.ds

        return self.ds
