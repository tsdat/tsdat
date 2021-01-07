import yaml
from typing import List, Dict

# TODO: add api method to download yaml templates or put them all
# in the examples folder.


class Keys():
    VARIABLES = 'variables'
    QC = 'qc'
    TESTS = 'tests'
    ALL = 'ALL'


class Variable:
    ALL = 'ALL'

    """
    Converts dictionary of properties into a class
    """
    def __init__(self, name: str, dictionary: Dict):
        self.name = name
        self.attrs = dictionary.get('attrs', {})
        self.dims = dictionary.get('dims', [])

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])


class QCTest:
    BAD = 'Bad'
    INDETERMINATE = 'Indeterminate'

    """
    Converts dictionary of properties into a class
    """
    def __init__(self, name: str, dictionary: Dict):
        self.name = name
        self.description = dictionary.get('description', None)
        self.qc_bit = dictionary.get('qc_bit', None)
        self.assessment = dictionary.get('assessment', None)
        self.variables = dictionary.get('variables', [])
        self.exclude = dictionary.get('exclude', [])
        self.operators = dictionary.get('operators', {})
        self.error_handlers = dictionary.get('error_handlers', {})

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])


class Config:
    """
    Wrapper for Dictionary of config values that provides helper functions for
    quick access.
    """

    def __init__(self, dictionary: Dict):
        self.dictionary = dictionary
        self._parse_variables(dictionary)
        self._parse_qc_tests(dictionary)

    @classmethod
    def load(self, filepaths: List[str]) -> object:
        """-------------------------------------------------------------------
        Load one or more yaml config files which define data following 
        mhkit-cloud data standards.
        
        TODO: add a schema validation check on yaml so users can know if the 
        file is valid
        
        Args:
            filepaths (List[str]): The paths to the config files to load

        Returns:
            Config: A Config instance created from the filepaths.
        -------------------------------------------------------------------"""
        config = dict()
        for filepath in filepaths:
            with open(filepath, 'r') as file:
                dict_list = list(yaml.load_all(file))
                for dictionary in dict_list:
                    config.update(dictionary)
        return Config(config)

    def get_variable_names(self):
        # Stupid python 3 returns keys as a dict_keys object.
        # Not really sure the purpose of this extra class :(.
        return list(self.variables.keys())

    def get_variable(self, variable_name):
        return self.variables.get(variable_name, None)

    def get_variables(self):
        return self.variables.values()

    def get_qc_test_names(self):
        # Stupid python 3 returns keys as a dict_keys object.
        # Not really sure the purpose of this extra class :(.
        return list(self.qc_tests.keys())

    def get_qc_test(self, test_name):
        return self.qc_tests.get(test_name, None)

    def get_qc_tests(self):
        return self.qc_tests.values()

    def _parse_qc_tests(self, dictionary):
        self.qc_tests = {}
        test_names = dictionary.get(Keys.QC, {}).get(Keys.TESTS, {}).keys()
        for test_name in test_names:
            test_dict = dictionary.get(Keys.QC, {}).get(Keys.TESTS, {}).get(test_name, None)
            if test_dict:
                self.qc_tests[test_name] = QCTest(test_name, test_dict)

    def _parse_variables(self, dictionary):
        self.variables = {}
        variable_names = dictionary.get(Keys.VARIABLES, {}).keys()
        for variable_name in variable_names:
            var_dict = dictionary.get(Keys.VARIABLES, {}).get(variable_name, None)
            if var_dict:
                self.variables[variable_name] = Variable(variable_name, var_dict)


