
import numpy as np
import xarray as xr

from typing import List

from tsdat.config import Config, QualityTestDefinition
from tsdat.constants import VARS
from tsdat.utils import DSUtil
from tsdat.config.utils import instantiate_handler


class QC(object):
    """-------------------------------------------------------------------
    Class that provides static helper functions for providing quality
    control checks on a tsdat-standardized xarray dataset.
    -------------------------------------------------------------------"""

    @staticmethod
    def apply_tests(ds: xr.Dataset, config: Config, previous_data: xr.Dataset) -> xr.Dataset:
        """-------------------------------------------------------------------
        Applies the QC tests defined in the given Config to this dataset.
        QC results will be embedded in the dataset.  QC metadata will be
        stored as attributes, and QC flags will be stored as a bitwise integer
        in new companion qc_ variables that are added to the dataset.
        This method will create QC companion variables if they don't exist.

        Args:
            ds (xr.Dataset): The dataset to apply tests to
            config (Config): A configuration definition (loaded from yaml)
            previous_data(xr.Dataset): A dataset from the previous processing
            interval (i.e., file).  This is used to check for consistency between
            files, such as for monitonic or delta checks when we need to check
            the previous value.

        Raises:
            QCError:  A QCError indicates that a fatal error has occurred.

        Returns:
            (xr.Dataset): The dataset after quality tests and handlers have
            been applied.

        -------------------------------------------------------------------"""

        # First run the coordinate variable tests, in order
        qc_tests: List[QualityTestDefinition] = config.get_qc_tests_coord()

        for qc_test in qc_tests:
            qc_checker = QCChecker(ds, config, qc_test, previous_data, coord=True)
            ds = qc_checker.run()

        # Then run the other variable qc tests, in order
        qc_tests: List[QualityTestDefinition] = config.get_qc_tests()

        for qc_test in qc_tests:
            qc_checker = QCChecker(ds, config, qc_test, previous_data)
            ds = qc_checker.run()

        return ds


class QCChecker:
    """-------------------------------------------------------------------
    Applies a single QC test to the given Dataset, as defined by the Config
    -------------------------------------------------------------------"""
    def __init__(self, ds: xr.Dataset,
                 config: Config,
                 test: QualityTestDefinition,
                 previous_data: xr.Dataset,
                 coord: bool = False):

        # Get the variables this test applies to
        variable_names = test.variables

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
        excludes = test.exclude
        for exclude in excludes:
            variable_names.remove(exclude)

        # Get the operator
        operator = instantiate_handler(ds, previous_data, test, handler_desc=test.operator)

        # Get the error handlers (optional)
        error_handlers = test.error_handlers

        self.ds = ds
        self.variable_names = variable_names
        self.operator = operator
        self.error_handlers = error_handlers
        self.test: QualityTestDefinition = test
        self.previous_data = previous_data
        self.coord = coord

    def run(self) -> xr.Dataset:
        """-------------------------------------------------------------------
        Runs the QC test for each specified variable

        Raises:
            QCError:  A QCError indicates that a fatal error has occurred.

        Returns:
            (xr.Dataset): The dataset after the quality operator and its 
            quality handlers have been run.
        -------------------------------------------------------------------"""
        for variable_name in self.variable_names:

            # Apply the operator
            results_array: np.ndarray = self.operator.run(variable_name)

            # If results_array is None, then we just skip this test
            if results_array is not None:

                # TODO: Only record qc if a qc bit has been defined
                # If this is not a coordinate variable, then record
                # the test results in a qc_ companion variable
                if not self.coord:
                    self.ds.qcfilter.add_test(
                        variable_name, index=results_array,
                        test_number=self.test.qc_bit,
                        test_meaning=self.test.meaning,
                        test_assessment=self.test.assessment)

                # If any values fail, then call any defined error handlers
                if np.sum(results_array) > 0 and self.error_handlers is not None:
                    for handler_name in self.error_handlers.keys():
                        handler_desc = {handler_name: self.error_handlers.get(handler_name)}
                        error_handler = instantiate_handler(self.ds, self.previous_data, self.test, handler_desc=handler_desc)[0]
                        error_handler.run(variable_name, results_array)
                        self.ds: xr.Dataset = error_handler.ds
                    self.operator.ds = self.ds

            else: 
                results_array = np.zeros_like(self.ds[variable_name].data) == 1

                if not self.coord:
                    self.ds.qcfilter.add_test(
                        variable_name, index=results_array,
                        test_number=self.test.qc_bit,
                        test_meaning="N/A",
                        test_assessment="Indeterminate")

        return self.ds