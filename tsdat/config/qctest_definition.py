from typing import Dict


class QCTestDefinition:
    """
    Converts dictionary of properties into a class
    """
    def __init__(self, name: str, dictionary: Dict):
        assert(dictionary is not None)
        self.name = name
        self.meaning = dictionary.get('meaning', None)
        self.qc_bit = dictionary.get('qc_bit', None)
        self.assessment = dictionary.get('assessment', None)
        self.variables = dictionary.get('variables', [])
        self.exclude = dictionary.get('exclude', [])
        self.operator = dictionary.get('operator', {})
        self.error_handlers = dictionary.get('error_handlers', {})

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])