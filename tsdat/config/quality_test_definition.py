from typing import Dict

class QualityManagerKeys:

    VARIABLES = 'variables'
    EXCLUDE = 'exclude'
    OPERATOR = 'checker'
    ERROR_HANDLERS = 'handlers'


class QualityManagerDefinition:
    """
    Converts dictionary of properties into a class
    """
    def __init__(self, name: str, dictionary: Dict):
        assert(dictionary is not None)
        self.name = name
        self.variables = dictionary.get(QualityManagerKeys.VARIABLES)
        self.exclude = dictionary.get(QualityManagerKeys.EXCLUDE, [])
        self.operator = dictionary.get(QualityManagerKeys.OPERATOR)
        self.error_handlers = dictionary.get(QualityManagerKeys.ERROR_HANDLERS, {})

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])
