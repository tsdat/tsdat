from typing import List
import importlib
import numpy as np
import xarray as xr
import act.qc.qcfilter
from tsdat import Config, QCTest, Variable


@xr.register_dataset_accessor('qc')
class QCAccessor(object):
    """-------------------------------------------------------------------
    Class that adds qc functionality to an XArray Dataset.  QC functions
    may be accessed via the 'qc' property.  For example:

    ds = xr.Dataset({'a': np.linspace(0, 10), 'b': np.linspace(0, 20)})
    ds.qc.apply_tests(config)
    -------------------------------------------------------------------"""
    def __init__(self, dataset):
        self.ds = dataset

    def apply_tests(self, config: Config):
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
        qc_tests: List[QCTest] = config.get_qc_tests()

        # TODO: we need to add default missing value/monotonic checks for
        # all coordinate variables if they have not already been defined.
        # If the user has overridden the coordinate variable test, then
        # we need to make sure that there is a Fail error handler assigned.
        for qc_test in qc_tests:
            qc_checker = QCChecker(self.ds, config, qc_test)
            qc_checker.run()


class QCChecker:
    """-------------------------------------------------------------------
    Applies a single QC test to the given Dataset, as defined by the Config
    -------------------------------------------------------------------"""
    def __init__(self, ds: xr.Dataset, config: Config, test: QCTest):
        # Get the variables this test applies to
        variable_names = test.variables
        if Variable.ALL in variable_names:
            variable_names = config.get_variable_names()

        # Exclude any excludes
        excludes = test.exclude
        for exclude in excludes:
            variable_names.remove(exclude)

        # Get the operators
        operators = self._instantiate_operators(ds, test, test.operators)

        # Get the error handlers
        error_handlers = self._instantiate_operators(ds, test, test.error_handlers)

        self.ds = ds
        self.variable_names = variable_names
        self.operators = operators
        self.error_handlers = error_handlers
        self.test: QCTest = test

    def run(self):
        """-------------------------------------------------------------------
        Runs the QC test for each specified variable

        Raises:
            QCError:  A QCError indicates that a fatal error has occurred.
        -------------------------------------------------------------------"""
        for variable_name in self.variable_names:

            # Apply the operators in order
            for operator in self.operators:
                results_array = operator.run(variable_name)

                # If results_array is None, then we just skip this test
                if results_array is not None:

                    # If any values fail, then call any defined error handlers
                    if sum(results_array) > 0:
                        for error_handler in self.error_handlers:
                            error_handler.run(variable_name, results_array)

                    # Record the test results in a qc_ companion variable
                    self.ds.qcfilter.add_test(
                        variable_name, index=results_array,
                        test_number=self.test.qc_bit,
                        test_meaning=self.test.description,
                        test_assessment=self.test.assessment)

    @staticmethod
    def _instantiate_operators(ds: xr.Dataset, test: QCTest, operators_dict):
        operators = []
        for operator_fq_name in operators_dict.keys():
            params = operators_dict.get(operator_fq_name, {})

            # Convert the class reference to an object
            module_name, class_name = QCChecker._parse_fully_qualified_name(operator_fq_name)
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            instance = class_(ds, test, params)
            operators.append(instance)

        return operators

    @staticmethod
    def _parse_fully_qualified_name(fully_qualified_name: str):
        module_name, class_name = fully_qualified_name.rsplit('.', 1)
        return module_name, class_name
