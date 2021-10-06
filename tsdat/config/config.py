import yaml
from yamllint import linter
from yamllint.config import YamlLintConfig
from typing import List, Dict
from .keys import Keys
from .pipeline_definition import PipelineDefinition
from .dataset_definition import DatasetDefinition
from .quality_manager_definition import QualityManagerDefinition


class Config:
    """
    Wrapper for the pipeline configuration file.

    Note: in most cases, ``Config.load(filepath)`` should be used to
    instantiate the Config class.

    :param dictionary: The pipeline configuration file as a dictionary.
    :type dictionary: Dict
    """

    def __init__(self, dictionary: Dict):
        pipeline_dict = dictionary.get(Keys.PIPELINE)
        dataset_dict = dictionary.get(Keys.DATASET_DEFINITION)
        quality_managers_dict = dictionary.get(Keys.QUALITY_MANAGEMENT, {})
        self.pipeline_definition = PipelineDefinition(pipeline_dict)
        self.dataset_definition = DatasetDefinition(
            dataset_dict, self.pipeline_definition.output_datastream_name
        )
        self.quality_managers = self._parse_quality_managers(quality_managers_dict)

    def _parse_quality_managers(
        self, dictionary: Dict
    ) -> Dict[str, QualityManagerDefinition]:
        """Extracts QualityManagerDefinitions from the config file.

        :param dictionary: The quality_management dictionary.
        :type dictionary: Dict
        :return: Mapping of quality manager name to QualityManagerDefinition
        :rtype: Dict[str, QualityManagerDefinition]
        """
        quality_managers: Dict[str, QualityManagerDefinition] = {}
        for manager_name, manager_dict in dictionary.items():
            quality_managers[manager_name] = QualityManagerDefinition(
                manager_name, manager_dict
            )
        return quality_managers

    @classmethod
    def load(self, filepaths: List[str]):
        """Load one or more yaml pipeline configuration files. Multiple files
        should only be passed as input if the pipeline configuration file is
        split across multiple files.

        :param filepaths: The path(s) to yaml configuration files to load.
        :type filepaths: List[str]
        :return: A Config object wrapping the yaml configuration file(s).
        :rtype: Config
        """
        if isinstance(filepaths, str):
            filepaths = [filepaths]
        config = dict()
        for filepath in filepaths:
            Config.lint_yaml(filepath)
            with open(filepath, "r") as file:
                dict_list = list(yaml.safe_load_all(file))
                for dictionary in dict_list:
                    config.update(dictionary)
        return Config(config)

    @staticmethod
    def lint_yaml(filename: str):
        """Lints a yaml file and raises an exception if an error is found.

        :param filename: The path to the file to lint.
        :type filename: str
        :raises Exception: Raises an exception if an error is found.
        """
        conf = YamlLintConfig(
            '{"extends": "relaxed", "rules": {"line-length": "disable", "trailing-spaces": "disable", "empty-lines": "disable", "new-line-at-end-of-file": "disable"}}'
        )
        with open(filename) as file:
            gen = linter.run(file, conf)
            errors = [error for error in gen if error.level == "error"]
            if errors:
                errors = "\n".join("\t\t" + str(error) for error in errors)
                raise Exception(
                    f"Syntax errors found in yaml file {filename}: \n{errors}"
                )
