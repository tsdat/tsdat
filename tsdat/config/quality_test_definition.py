from typing import Dict

class QCTestKeys:

    MEANING = 'meaning'
    CORRECTION = 'correction'
    QC_BIT = 'bit'
    ASSESSMENT = 'assessment'
    VARIABLES = 'variables'
    EXCLUDE = 'exclude'
    OPERATOR = 'checker'
    ERROR_HANDLERS = 'handlers'


class QualityTestDefinition:
    """
    Converts dictionary of properties into a class
    """
    def __init__(self, name: str, dictionary: Dict):
        assert(dictionary is not None)
        self.name = name
        self.meaning = dictionary.get(QCTestKeys.MEANING, None)
        self.correction = dictionary.get(QCTestKeys.CORRECTION, None)
        self.qc_bit = dictionary.get(QCTestKeys.QC_BIT, None)
        self.assessment = dictionary.get(QCTestKeys.ASSESSMENT, None)
        self.variables = dictionary.get(QCTestKeys.VARIABLES, [])
        self.exclude = dictionary.get(QCTestKeys.EXCLUDE, [])
        self.operator = dictionary.get(QCTestKeys.OPERATOR)
        self.error_handlers = dictionary.get(QCTestKeys.ERROR_HANDLERS, {})

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])