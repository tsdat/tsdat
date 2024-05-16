from pathlib import Path
from typing import (
    Any,
    Dict,
    Optional,
)

from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    BaseModel,
    ValidationError,
)

from .config_error import ConfigError
from .read_yaml import read_yaml


class YamlModel(BaseModel):
    @classmethod
    def from_yaml(cls, filepath: Path, overrides: Optional[Dict[str, Any]] = None):
        """------------------------------------------------------------------------------------
        Creates a python configuration object from a yaml file.

        Args:
            filepath (Path): The path to the yaml file
            overrides (Optional[Dict[str, Any]], optional): Overrides to apply to the
                yaml before instantiating the YamlModel object. Defaults to None.

        Returns:
            YamlModel: A YamlModel subclass

        ------------------------------------------------------------------------------------
        """
        config = read_yaml(filepath)
        if overrides:
            for pointer, new_value in overrides.items():
                set_pointer(config, pointer, new_value)
        try:
            return cls(**config)
        except (ValidationError, Exception) as e:
            raise ConfigError(
                f"Error encountered while instantiating {filepath}"
            ) from e

    @classmethod
    def generate_schema(cls, output_file: Path):
        """------------------------------------------------------------------------------------
        Generates JSON schema from the model fields and type annotations.

        Args:
            output_file (Path): The path to store the JSON schema.

        ------------------------------------------------------------------------------------
        """
        output_file.write_text(cls.schema_json(indent=4))
