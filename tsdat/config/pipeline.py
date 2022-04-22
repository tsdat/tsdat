from pathlib import Path
from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    BaseSettings,
    Extra,
    Field,
    validator,
)
from pydantic.fields import ModelField
from typing import Any, Dict, List, Pattern, Union

from ..config.retriever import RetrieverConfig
from .dataset import DatasetConfig
from .quality import QualityConfig
from .storage import StorageConfig
from .utils import (
    ParametrizedConfigClass,
    YamlModel,
    Overrideable,
    read_yaml,
    matches_overrideable_schema,
    recusive_instantiate,
)

__all__ = ["PipelineConfig"]


class ConfigSettings(BaseSettings, extra=Extra.allow):
    validate_retriever_config: bool = Field(
        True,
        # TODO: description
    )
    validate_dataset_config: bool = Field(
        True,
        description="Validate the dataset configuration file after any overrides have"
        " been merged. Disabling validation is generally 10-30x faster, but comes with"
        " some risks and can easily lead to buggy behavior if you are not careful. For"
        " example. the dataset configuration model uses validators to set defaults for"
        " the 'datastream' attribute based on other global attributes that are set. The"
        " 'datastream' attribute is used elsewhere in the pipeline as a label for the"
        " dataset, and is used by some storage classes to generate the filepath where"
        " the data should be saved. If you disable dataset configuration validation,"
        " THIS WILL NOT WORK, so you will need to take care to set the 'datastream'"
        " attribute manually in your config file directly. Because of the complicated"
        " nature of dataset configuration files, it is almost always better to leave"
        " validation ON.",
    )
    validate_quality_config: bool = Field(
        True,
        description="Validate the quality configuration file after any overrides have"
        " been merged. Disabling validation is generally 10-30x faster, but comes with"
        " some risks and can easily lead to buggy behavior if you are not careful. For"
        " example. the quality configuration model uses validators to set defaults for"
        " regex patterns in the registry/readers section. If you disable validation of"
        " the quality configuration file, then you must ensure that your regex patterns"
        " are set explicitly, as you will not be able to rely on the dynamic defaults.",
    )
    validate_storage_config: bool = Field(
        True,
        description="Validate the storage configuration file after any overrides have"
        " been merged. Disabling validation is generally 10-30x faster, but comes with"
        " some risks and can easily lead to buggy behavior if you are not careful.",
    )


class PipelineConfig(ParametrizedConfigClass, YamlModel, extra=Extra.forbid):
    """------------------------------------------------------------------------------------
    Class used read in the yaml pipeline config file and to generate its json schema for
    early validation of its properties.

    This class also provides a method to instantiate a subclass of tsdat.pipeline.Pipeline
    from the parsed pipeline configuration file.
    ------------------------------------------------------------------------------------"""

    # TODO: Provide a way of instantiating only the associations (so we can quickly
    # determine which pipeline should be used for a given input)
    # TODO: Add a root validator to ensure that properties from the quality config align
    # with dataset config properties -- e.g., includes / excludes are real variables

    # HACK: Pattern[str] type is correct, but doesn't work with pydantic v1.9.0
    triggers: List[Pattern] = Field(  # type: ignore
        description="A list of regex patterns matching input keys to determine if the"
        " pipeline should be run. Please ensure these are specific as possible in order"
        " to match the desired input keys without any false positive matches (this is"
        " more important in repositories with many pipelines)."
    )
    settings: ConfigSettings = ConfigSettings()  # type: ignore

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
    @classmethod
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

        # NOTE: values["settings"] is a ConfigSettings object because the settings
        # property comes before dataset/quality/storage. Order matters in this case.
        settings: ConfigSettings = values["settings"]
        validate = getattr(settings, f"validate_{field.name}_config")
        if not validate:
            return config_cls.construct(**v)

        return config_cls(**v)

    def instaniate_pipeline(self) -> Any:
        """------------------------------------------------------------------------------------
        This method instantiates the tsdat.pipeline.BasePipeline subclass referenced by the
        classname property on the PipelineConfig instance and passes all properties on the
        PipelineConfig class (except for 'classname') as keyword arguments to the constructor
        of the tsdat.pipeline.BasePipeline subclass.

        Properties and sub-properties of the PipelineConfig class that are subclasses of
        tsdat.config.utils.ParametrizedConfigClass (e.g, classes that define a 'classname' and
        optional 'parameters' properties) will also be instantiated in similar fashion. See
        tsdat.config.utils.recursive_instantiate for implementation details.


        Returns:
            Pipeline: An instance of a tsdat.pipeline.Pipeline subclass.

        ------------------------------------------------------------------------------------"""
        return recusive_instantiate(self)
