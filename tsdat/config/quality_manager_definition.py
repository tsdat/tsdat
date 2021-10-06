from typing import Dict


class QualityManagerKeys:
    """Class that provides a handle for keys in the quality management section
    of the pipeline config file."""

    VARIABLES = "variables"
    EXCLUDE = "exclude"
    CHECKER = "checker"
    HANDLERS = "handlers"


class QualityManagerDefinition:
    """Wrapper for the quality_management portion of the pipeline config
    file.

    :param name: The name of the quality manager in the config file.
    :type name: str
    :param dictionary:
        The dictionary contents of the quality manager from the config
        file.
    :type dictionary: Dict
    """

    def __init__(self, name: str, dictionary: Dict):
        assert dictionary is not None
        self.name = name
        self.variables = dictionary.get(QualityManagerKeys.VARIABLES)
        self.exclude = dictionary.get(QualityManagerKeys.EXCLUDE, [])
        self.checker = dictionary.get(QualityManagerKeys.CHECKER)
        self.handlers = dictionary.get(QualityManagerKeys.HANDLERS, {})

        # Now set any other properties that may have been added by user
        for key in dictionary:
            if not hasattr(self, key):
                setattr(self, key, dictionary[key])
