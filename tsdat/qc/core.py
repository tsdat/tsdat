from typing import List
import importlib
from tsdat import Config, TimeSeriesDataset, QCTest, Variable


def apply_qc(config: Config, tsds: TimeSeriesDataset):
    """
    Operates directly on the provided dataset
    to apply qc checks defined in the config.  QC
    results will be embedded in the dataset.  QC
    metadata will be stored as attributes, and
    QC flags will be stored as a bitwise integer in
    new companion qc_ variables that are added to the
    dataset.
    """

    # Iterate through the tests in order
    qc_tests: List[QCTest] = config.get_qc_tests()
    for test in qc_tests:
        qc_checker = QCChecker(tsds, config, test)
        qc_checker.run()


class QCChecker:
    def __init__(self, tsds: TimeSeriesDataset, config: Config, test: QCTest):
        # Get the variables this test applies to
        variable_names = test.variables
        if Variable.ALL in variable_names:
            variable_names = config.get_variable_names()

        # Exclude any excludes
        excludes = test.exclude
        for exclude in excludes:
            variable_names.remove(exclude)

        # Get the operators
        operators = self._instantiate_operators(tsds, test.operators)

        # Get the error handlers
        error_handlers = self._instantiate_operators(tsds, test.error_handlers)

        self.tsds = tsds
        self.variable_names = variable_names
        self.operators = operators
        self.error_handlers = error_handlers
        self.test: QCTest = test

    def run(self):
        """
        Runs the QC test for each specified variable
        """

        for variable_name in self.variable_names:
            succeeded = True

            # Get the variable from the dataset
            var = self.tsds.get_var(variable_name)

            # Create the QC var if needed and add bit metadata for this test
            self._add_bit_attributes(variable_name)

            # Iterate over data values (depends on # of dims)
            dims, lengths = self.tsds.get_shape(variable_name)
            coordinates = []

            if len(dims) == 1:
                coordinates = [0] * 1
                for x in range(lengths[0]):
                    value = var.values[x]
                    coordinates[0] = x
                    succeeded &= self._apply_test(variable_name, value, coordinates)

            elif len(dims) == 2:
                coordinates = [0] * 2
                for x in range(lengths[0]):
                    coordinates[0] = x
                    for y in range(lengths[1]):
                        coordinates[1] = y
                        value = var.values[x][y]
                        succeeded &= self._apply_test(variable_name, value, coordinates)

            elif len(dims) == 3:
                coordinates = [0] * 3
                for x in range(lengths[0]):
                    coordinates[0] = x
                    for y in range(lengths[1]):
                        coordinates[1] = y
                        for z in range(lengths[2]):
                            coordinates[2] = z
                            value = var.values[x][y][z]
                            succeeded &= self._apply_test(variable_name, value, coordinates)

    def get_bitmask(self):
        bit_position = int(self.test.qc_bit)

        # create a string representation of the bits
        binary_str = '00000000000000000000000000000000'
        binary_arr = list(binary_str)

        # update the character at position(len(binary_arr) - bit_position)
        # i.e., 00000000000000000000000000000100 for bit position 3
        char_position = len(binary_arr) - bit_position
        binary_arr[char_position] = '1'

        # convert binary string back to integer
        binary_str = "".join(binary_arr)
        return int(binary_str, 2)

    def _apply_test(self, variable_name, value, coordinates):
        """
        Apply a qc check on one value.  The coordinates will show how many dimensions this
        variable has.
        """
        all_succeeded = True

        # For each value,Apply the operators in order
        for operator in self.operators:
            success = operator.run(variable_name, coordinates, value)

            if not success:
                all_succeeded = False
                self._handle_failure(variable_name, coordinates)
                break

        return all_succeeded

    def _add_bit_attributes(self, variable_name):
        qc_var = self.tsds.get_or_create_qc_var(variable_name)
        bit = self.test.qc_bit
        mask = self.get_bitmask()

        self._add_qc_bit_value(qc_var, 'flag_masks', mask)
        self._add_qc_bit_value(qc_var, 'flag_bits', bit)
        self._add_qc_bit_value(qc_var, 'flag_meanings', self.test.description)
        self._add_qc_bit_value(qc_var, 'flag_assessments', self.test.assessment)

    @staticmethod
    def _add_qc_bit_value(qc_var, attr_name, attr_value):
        attr_array = qc_var.attrs.get(attr_name, [])
        attr_array.append(attr_value)
        qc_var.attrs[attr_name] = attr_array

    def _handle_failure(self, variable_name, coordinates):
        # Fail this variable
        bitmask = self.get_bitmask()
        self._fail_variable(bitmask, variable_name, coordinates)

        # If this is a coordinate variable, then fail all the
        # associated data variables as well at that same coordinate
        if self.tsds.is_coord_var(variable_name):

            # If this is a coordinate variable, then the variable name is the
            # dimension name
            dim_name = variable_name

            # If this is a coordinate variable, then there will be only one coordinate for that dim
            dim_coordinate = coordinates[0]
            data_variable_names = self.tsds.get_variables_with_dimension(dim_name)
            for data_variable_name in data_variable_names:

                data_var_dims, data_var_lengths = self.tsds.get_shape(data_variable_name)
                dim_axis = data_var_dims.index(dim_name)

                if len(data_var_dims) == 1:
                    # this var has only one dimension, so use the coordinates of the coordinate var
                    self._fail_variable(bitmask, data_variable_name, coordinates)

                elif len(data_var_dims) == 2 and dim_axis == 0:
                    data_var_coordinates = [0] * 2

                    # We fix dimension 1 to the dim_coordinate and vary the second dimension idx
                    data_var_coordinates[0] = dim_coordinate
                    for y in range(data_var_lengths[1]):
                        data_var_coordinates[1] = y
                        self._fail_variable(bitmask, data_variable_name, data_var_coordinates)

                elif len(data_var_dims) == 2 and dim_axis == 1:
                    data_var_coordinates = [0] * 2

                    # We fix dimension 2 to the dim_coordinate and vary the first dimension idx
                    data_var_coordinates[1] = dim_coordinate
                    for x in range(data_var_lengths[0]):
                        data_var_coordinates[0] = x
                        self._fail_variable(bitmask, data_variable_name, data_var_coordinates)

                elif len(data_var_dims) == 3 and dim_axis == 0:
                    data_var_coordinates = [0] * 3

                    # We fix dimension 1 to the dim_coordinate and vary the second & third dimension idxes
                    data_var_coordinates[0] = dim_coordinate
                    for y in range(data_var_lengths[1]):
                        data_var_coordinates[1] = y
                        for z in range(data_var_lengths[2]):
                            data_var_coordinates[2] = z
                            self._fail_variable(bitmask, data_variable_name, data_var_coordinates)

                elif len(data_var_dims) == 3 and dim_axis == 1:
                    data_var_coordinates = [0] * 3

                    # We fix dimension 2 to the dim_coordinate and vary the first & third dimension idxes
                    data_var_coordinates[1] = dim_coordinate
                    for x in range(data_var_lengths[0]):
                        data_var_coordinates[0] = x
                        for z in range(data_var_lengths[2]):
                            data_var_coordinates[2] = z
                            self._fail_variable(bitmask, data_variable_name, data_var_coordinates)

                elif len(data_var_dims) == 3 and dim_axis == 2:
                    data_var_coordinates = [0] * 3

                    # We fix dimension 3 to the dim_coordinate and vary the first & second dimension idxes
                    data_var_coordinates[2] = dim_coordinate
                    for x in range(data_var_lengths[0]):
                        data_var_coordinates[0] = x
                        for y in range(data_var_lengths[1]):
                            data_var_coordinates[1] = y
                            self._fail_variable(bitmask, data_variable_name, data_var_coordinates)

    def _fail_variable(self, bitmask, variable_name, coordinates):

        qc_var = self.tsds.get_or_create_qc_var(variable_name)

        # First set the bit flags for the qc_variable
        if len(coordinates) == 1:
            x = coordinates[0]
            qc_var.values[x] = qc_var.values[x] | bitmask

        elif len(coordinates) == 2:
            x = coordinates[0]
            y = coordinates[1]
            qc_var.values[x][y] = qc_var.values[x][y] | bitmask

        elif len(coordinates) == 3:
            x = coordinates[0]
            y = coordinates[1]
            z = coordinates[2]
            qc_var.values[x][y][z] = qc_var.values[x][y][z] | bitmask

        for error_handler in self.error_handlers:
            error_handler.run(variable_name, coordinates)

    @staticmethod
    def _instantiate_operators(tsds: TimeSeriesDataset, operators_dict):
        operators = []
        for operator_fq_name in operators_dict.keys():
            params = operators_dict.get(operator_fq_name, {})

            # Convert the class reference to an object
            module_name, class_name = QCChecker._parse_fully_qualified_name(operator_fq_name)
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            instance = class_(tsds, params)
            operators.append(instance)

        return operators

    @staticmethod
    def _parse_fully_qualified_name(fully_qualified_name: str):
        module_name, class_name = fully_qualified_name.rsplit('.', 1)
        return module_name, class_name
