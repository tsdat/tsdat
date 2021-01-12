from typing import List
import importlib
import xarray as xr
import act
from tsdat.config import Config, QCTestDefinition, VariableDefinition
from tsdat.constants import VARS
from tsdat.utils import DSUtil


class QC(object):
    """-------------------------------------------------------------------
    Class that provides static helper functions for providing quality
    control checks on a tsdat-standardized xarray dataset.
    -------------------------------------------------------------------"""

    @staticmethod
    def apply_tests(ds: xr.Dataset, config: Config, previous_data: xr.Dataset):
        """-------------------------------------------------------------------
        Applies the QC tests defined in the given Config to this dataset.
        QC results will be embedded in the dataset.  QC metadata will be
        stored as attributes, and QC flags will be stored as a bitwise integer
        in new companion qc_ variables that are added to the dataset.
        This method will create QC companion variables if they don't exist.

        Args:
            config (Config): A configuration definition (loaded from yaml)

        Raises:
            QCError:  A QCError indicates that a fatal error has occurred.

        -------------------------------------------------------------------"""

        # Iterate through the tests in order
        qc_tests: List[QCTestDefinition] = config.get_qc_tests()

        for qc_test in qc_tests:
            qc_checker = QCChecker(ds, config, qc_test, previous_data)
            qc_checker.run()


class QCChecker:
    """-------------------------------------------------------------------
    Applies a single QC test to the given Dataset, as defined by the Config
    -------------------------------------------------------------------"""
    def __init__(self, ds: xr.Dataset, config: Config, test: QCTestDefinition, previous_data: xr.Dataset):
        # Get the variables this test applies to
        variable_names = test.variables
        if VARS.ALL in variable_names:
            variable_names = DSUtil.get_non_qc_variables(ds)

        # Exclude any excludes
        excludes = test.exclude
        for exclude in excludes:
            variable_names.remove(exclude)

        # Get the operator
        operator = self._instantiate_class(ds, previous_data, test, test.operator)

        # Get the error handler (optional)
        error_handler = self._instantiate_class(ds, previous_data, test, test.error_handler)

        self.ds = ds
        self.variable_names = variable_names
        self.operator = operator
        self.error_handler = error_handler
        self.test: QCTestDefinition = test
        self.previous_data = previous_data

    def run(self):
        """-------------------------------------------------------------------
        Runs the QC test for each specified variable

        Raises:
            QCError:  A QCError indicates that a fatal error has occurred.
        -------------------------------------------------------------------"""
        for variable_name in self.variable_names:

            # Apply the operator
            results_array = self.operator.run(variable_name)

            # If results_array is None, then we just skip this test
            if results_array is not None:

                # If any values fail, then call any defined error handlers
                if sum(results_array) > 0 and self.error_handler is not None:
                    self.error_handler.run(variable_name, results_array)

                # Record the test results in a qc_ companion variable
                self.ds.qcfilter.add_test(
                    variable_name, index=results_array,
                    test_number=self.test.qc_bit,
                    test_meaning=self.test.description,
                    test_assessment=self.test.assessment)

    @staticmethod
    def _instantiate_class(ds: xr.Dataset, previous_data: xr.Dataset, test: QCTestDefinition, class_desc):
        operator = None
        if class_desc is not None:
            params = class_desc.get('parameters', {})

            # Convert the class reference to an object
            module_name, class_name = QCChecker._parse_fully_qualified_name(class_desc['classname'])
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            instance = class_(ds, previous_data, test, params)
            operator = instance

        return operator

    @staticmethod
    def _parse_fully_qualified_name(fully_qualified_name: str):
        module_name, class_name = fully_qualified_name.rsplit('.', 1)
        return module_name, class_name
