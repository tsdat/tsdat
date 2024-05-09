from pathlib import Path
from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    Extra,
    Field,
    validator,
)
from pydantic.fields import ModelField
from typing import Any, Dict, List, Pattern, Union

from ..dataset import DatasetConfig
from ..quality import QualityConfig
from ..storage import StorageConfig
from ..utils import (
    ParameterizedConfigClass,
    YamlModel,
    Overrideable,
    read_yaml,
    matches_overrideable_schema,
    recursive_instantiate,
)
from ...config.retriever import RetrieverConfig
from ...pipeline.base import Pipeline


class PipelineConfig(ParameterizedConfigClass, YamlModel, extra=Extra.allow):
    """---------------------------------------------------------------------------------
    Contains configuration parameters for tsdat pipelines.

    This class is ultimately converted into a tsdat.pipeline.base.Pipeline subclass that
    will be used to process data.

    Provides methods to support yaml parsing and validation, including the generation of
    json schema for immediate validation. This class also provides a method to
    instantiate a tsdat.pipeline.base.Pipeline subclass from a parsed configuration
    file.

    Args:
        classname (str): The dotted module path to the pipeline that the specified
            configurations should apply to. To use the built-in IngestPipeline, for
            example, you would set 'tsdat.pipeline.pipelines.IngestPipeline' as the
            classname.
        triggers (List[Pattern[str]]): A list of regex patterns that should trigger this
            pipeline when matched with an input key.
        retriever (Union[Overrideable[RetrieverConfig], RetrieverConfig]): Either the
            path to the retriever configuration yaml file and any overrides that should
            be applied, or the retriever configurations themselves.
        dataset (Union[Overrideable[DatasetConfig], DatasetConfig]): Either the path to
            the dataset configuration yaml file and any overrides that should be
            applied, or the dataset configurations themselves.
        quality (Union[Overrideable[QualityConfig], QualityConfig]): Either the path to
            the quality configuration yaml file and any overrides that should be
            applied, or the quality configurations themselves.
        storage (Union[Overrideable[StorageConfig], StorageConfig]): Either the path to
            the storage configuration yaml file and any overrides that should be
            applied, or the storage configurations themselves.

    ---------------------------------------------------------------------------------"""

    # IDEA: Add a root validator to ensure that properties from the quality config align
    # with dataset config properties -- e.g., includes / excludes are real variables,
    # all retrieved variables are defined in the output dataset, etc.

    triggers: List[Pattern] = Field(  # type: ignore
        description="A list of regex patterns matching input keys to determine if the"
        " pipeline should be run. Please ensure these are specific as possible in order"
        " to match the desired input keys without any false positive matches (this is"
        " more important in repositories with many pipelines)."
    )

    # Overrideable is used to trick pydantic into letting us generate json schema for
    # these objects, but during construction these are converted into the actual
    # DatasetConfig, QualityConfig, and StorageConfig objects.
    retriever: Union[Overrideable[RetrieverConfig], RetrieverConfig] = Field(
        description="Specify the retrieval configurations that the pipeline should use."
    )
    dataset: Union[Overrideable[DatasetConfig], DatasetConfig] = Field(
        description="Specify the dataset configurations that describe the structure and"
        " metadata of the dataset produced by this pipeline.",
    )
    quality: Union[Overrideable[QualityConfig], QualityConfig] = Field(
        description="Specify the quality checks and controls that should be applied to"
        " the dataset as part of this pipeline."
    )
    storage: Union[Overrideable[StorageConfig], StorageConfig] = Field(
        description="Specify the Storage configurations that should be used to save"
        " data produced by this pipeline."
    )

    @validator("retriever", "dataset", "quality", "storage", pre=True)
    def merge_overrideable_yaml(
        cls, v: Dict[str, Any], values: Dict[str, Any], field: ModelField
    ):
        object_field_mapping = {
            "retriever": RetrieverConfig,
            "dataset": DatasetConfig,
            "quality": QualityConfig,
            "storage": StorageConfig,
        }
        config_cls = object_field_mapping[field.name]

        if matches_overrideable_schema(v):
            defaults = read_yaml(Path(v["path"]))
            overrides = v.get("overrides", {})
            for pointer, new_value in overrides.items():
                set_pointer(defaults, pointer, new_value)
            v = defaults

        return config_cls(**v)

    def instantiate_pipeline(self) -> Pipeline:
        """------------------------------------------------------------------------------------
        Loads the tsdat.pipeline.BasePipeline subclass specified by the classname property.

        Properties and sub-properties of the PipelineConfig class that are subclasses of
        tsdat.config.utils.ParameterizedConfigClass (e.g, classes that define a 'classname' and
        optional 'parameters' properties) will also be instantiated in similar fashion. See
        tsdat.config.utils.recursive_instantiate for implementation details.


        Returns:
            Pipeline: An instance of a tsdat.pipeline.base.Pipeline subclass.

        ------------------------------------------------------------------------------------
        """
        return recursive_instantiate(self)
