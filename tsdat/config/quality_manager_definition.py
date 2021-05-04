from typing import Dict

class QualityManagerKeys:

    VARIABLES = 'variables'
    EXCLUDE = 'exclude'
    CHECKER = 'checker'
    HANDLERS = 'handlers'


class QualityManagerDefinition:
    """
    Converts dictionary of properties into a class
    """
    def __init__(self, name: str, dictionary: Dict):
        assert(dictionary is not None)
        self.name = name
        self.variables = dictionary.get(QualityManagerKeys.VARIABLES)
        self.exclude = dictionary.get(QualityManagerKeys.EXCLUDE, [])
        self.checker = dictionary.get(QualityManagerKeys.CHECKER)
        self.handlers = dictionary.get(QualityManagerKeys.HANDLERS, {})

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])
