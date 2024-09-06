from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern, Union

from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    Extra,
    Field,
    ValidationError,
    root_validator,
)
from typing_extensions import Self

from ...config.retriever import RetrieverConfig
from ...pipeline.base import Pipeline
from ..dataset import DatasetConfig
from ..quality import QualityConfig
from ..storage import StorageConfig
from ..utils import (
    ConfigError,
    Overrideable,
    ParameterizedConfigClass,
    matches_overridable_schema,
    read_yaml,
    recursive_instantiate,
)


def get_resolved_cfg_path(
    linked_path: str | Path, pipeline_cfg_path: str | Path | None
) -> Path:
    if pipeline_cfg_path is not None and (
        str(linked_path).startswith("../") or str(linked_path).startswith("./")
    ):
        return (Path(pipeline_cfg_path).parent / linked_path).resolve()
    return Path(linked_path)


class PipelineConfig(ParameterizedConfigClass, extra=Extra.allow):
    """Contains configuration parameters for tsdat pipelines.

    This class is ultimately converted into a tsdat.pipeline.base.Pipeline subclass that
    will be used to process data.

    Provides methods to support yaml parsing and validation, including the generation of
    json schema for immediate validation. This class also provides a method to
    instantiate a tsdat.pipeline.base.Pipeline subclass from a parsed configuration
    file."""

    # IDEA: Add a root validator to ensure that properties from the quality config align
    # with dataset config properties -- e.g., includes / excludes are real variables,
    # all retrieved variables are defined in the output dataset, etc.

    triggers: List[Pattern] = Field(  # type: ignore
        description="A list of regex patterns matching input keys to determine if the"
        " pipeline should be run. Please ensure these are specific as possible in order"
        " to match the desired input keys without any false positive matches (this is"
        " more important in repositories with many pipelines)."
    )
    """A list of regex patterns that should trigger this pipeline when matched with an
    input key."""

    # Overrideable is used to trick pydantic into letting us generate json schema for
    # these objects, but during construction these are converted into the actual
    # DatasetConfig, QualityConfig, and StorageConfig objects.
    retriever: Union[Overrideable[RetrieverConfig], RetrieverConfig] = Field(
        description="Specify the retrieval configurations that the pipeline should use."
    )
    """Either the path to the retriever configuration yaml file and any overrides that
    should be applied, or the retriever configurations themselves."""

    dataset: Union[Overrideable[DatasetConfig], DatasetConfig] = Field(
        description="Specify the dataset configurations that describe the structure and"
        " metadata of the dataset produced by this pipeline.",
    )
    """Either the path to the dataset configuration yaml file and any overrides that
    should be applied, or the dataset configurations themselves."""

    quality: Union[Overrideable[QualityConfig], QualityConfig] = Field(
        description="Specify the quality checks and controls that should be applied to"
        " the dataset as part of this pipeline."
    )
    """Either the path to the quality configuration yaml file and any overrides that
    should be applied, or the quality configurations themselves."""

    storage: Union[Overrideable[StorageConfig], StorageConfig] = Field(
        description="Specify the Storage configurations that should be used to save"
        " data produced by this pipeline."
    )
    """Either the path to the storage configuration yaml file and any overrides that
    should be applied, or the storage configurations themselves."""

    cfg_filepath: Optional[Path] = None
    """The path to the yaml config file used to instantiate this class. Set via the
    'from_yaml()' classmethod"""

    @root_validator(pre=True)
    def merge_overridable_yaml(cls, values: Dict[str, Any]):
        object_field_mapping = {
            "retriever": RetrieverConfig,
            "dataset": DatasetConfig,
            "quality": QualityConfig,
            "storage": StorageConfig,
        }
        for field, config_cls in object_field_mapping.items():
            v = values[field]
            if matches_overridable_schema(v):
                cfg_path = get_resolved_cfg_path(v["path"], values.get("cfg_filepath"))
                defaults = read_yaml(cfg_path)
                overrides = v.get("overrides", {})
                for pointer, new_value in overrides.items():
                    set_pointer(defaults, pointer, new_value)
                v = defaults
            values[field] = config_cls(**v)
        return values

    @classmethod
    def from_yaml(
        cls, filepath: Path, overrides: Optional[Dict[str, Any]] = None
    ) -> Self:
        """Creates a python configuration object from a yaml file.

        Args:
            filepath (Path): The path to the yaml file
            overrides (Optional[Dict[str, Any]], optional): Overrides to apply to the
                yaml before instantiating the YamlModel object. Defaults to None.

        Returns:
            YamlModel: A YamlModel subclass

        """
        config = read_yaml(filepath)
        if overrides:
            for pointer, new_value in overrides.items():
                set_pointer(config, pointer, new_value)
        try:
            return cls(cfg_filepath=filepath, **config)
        except (ValidationError, Exception) as e:
            raise ConfigError(
                f"Error encountered while instantiating {filepath}"
            ) from e

    @classmethod
    def generate_schema(cls, output_file: Path):
        """Generates JSON schema from the model fields and type annotations.

        Args:
            output_file (Path): The path to store the JSON schema.
        """
        output_file.write_text(cls.schema_json(indent=4))

    def instantiate_pipeline(self) -> Pipeline:
        """Loads the tsdat.pipeline.BasePipeline subclass specified by the classname property.

        Properties and sub-properties of the PipelineConfig class that are subclasses of
        tsdat.config.utils.ParameterizedConfigClass (e.g, classes that define a 'classname' and
        optional 'parameters' properties) will also be instantiated in similar fashion. See
        tsdat.config.utils.recursive_instantiate for implementation details.

        Returns:
            Pipeline: An instance of a tsdat.pipeline.base.Pipeline subclass.
        """
        return recursive_instantiate(self)
