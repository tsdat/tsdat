from pathlib import Path
from jsonpointer import set_pointer
from pydantic import (
    BaseSettings,
    Extra,
    Field,
    validator,
)
from typing import Any, Dict, List, Pattern, Union
from .dataset import DatasetDefinition
from .quality import QualityDefinition
from .storage import StorageConfig
from .utils import (
    ParametrizedClass,
    YamlModel,
    Overrideable,
    get_yaml,
    matches_overrideable_schema,
    recusive_instantiate,
)


class ConfigSettings(BaseSettings, extra=Extra.allow):
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


class PipelineConfig(ParametrizedClass, YamlModel, extra=Extra.forbid):
    """------------------------------------------------------------------------------------
    Class used read in the yaml pipeline config file and to generate its json schema for
    early validation of its properties.

    This class also provides a method to instantiate a subclass of tsdat.pipeline.Pipeline
    from the parsed pipeline configuration file.
    ------------------------------------------------------------------------------------"""

    # HACK: Pattern[str] type is correct, but doesn't work with pydantic v1.9.0
    associations: List[Pattern]  # type: ignore
    settings: ConfigSettings = ConfigSettings()  # type: ignore

    # HACK: Overrideable is used to trick pydantic into letting us generate json schema
    # for these objects, but during construction these are converted into the actual
    # DatasetDefinition, QualityDefinition, and StorageConfig objects.
    dataset: Union[Overrideable[DatasetDefinition], DatasetDefinition]
    quality: Union[Overrideable[QualityDefinition], QualityDefinition]
    storage: Union[Overrideable[StorageConfig], StorageConfig]

    @validator("dataset", pre=True)
    @classmethod
    def resolve_dataset_configurations(
        cls, v: Dict[str, Any], values: Dict[str, Any], **kwargs: Any
    ) -> DatasetDefinition:
        # Load YAML, merge overrides
        if matches_overrideable_schema(v):
            defaults = get_yaml(Path(v["path"]))
            overrides = v.get("overrides", {})
            for pointer, new_value in overrides.items():
                set_pointer(defaults, pointer, new_value)
            v = defaults
        # NOTE: values["settings"] is a ConfigSettings object because the settings
        # property comes before dataset/quality/storage. Order matters for these:
        # https://pydantic-docs.helpmanual.io/usage/models/#field-ordering
        settings: ConfigSettings = values["settings"]
        validate = settings.validate_dataset_config
        if not validate:
            return DatasetDefinition.construct(**v)
        return DatasetDefinition(**v)

    @validator("quality", pre=True)
    @classmethod
    def resolve_quality_configurations(
        cls, v: Dict[str, Any], values: Dict[str, Any], **kwargs: Any
    ) -> QualityDefinition:
        # Load YAML, merge overrides
        if matches_overrideable_schema(v):
            defaults = get_yaml(Path(v["path"]))
            overrides = v.get("overrides", {})
            for pointer, new_value in overrides.items():
                set_pointer(defaults, pointer, new_value)
            v = defaults

        settings: ConfigSettings = values["settings"]
        validate = settings.validate_quality_config
        if not validate:
            return QualityDefinition.construct(**v)
        return QualityDefinition(**v)

    @validator("storage", pre=True)
    @classmethod
    def resolve_storage_configurations(
        cls, v: Dict[str, Any], values: Dict[str, Any], **kwargs: Any
    ) -> StorageConfig:
        # Load YAML, merge overrides
        if matches_overrideable_schema(v):
            defaults = get_yaml(Path(v["path"]))
            overrides = v.get("overrides", {})
            for pointer, new_value in overrides.items():
                set_pointer(defaults, pointer, new_value)
            v = defaults

        settings: ConfigSettings = values["settings"]
        validate = settings.validate_storage_config
        if not validate:
            return StorageConfig.construct(**v)
        return StorageConfig(**v)

    # TODO: Add a root validator to ensure that properties from the quality config align
    # with dataset config properties

    # TODO: Provide a way of instantiating only the associations (so we can quickly
    # determine which pipeline should be used for a given input)
    @classmethod
    def from_yaml(cls, filepath: Path, validate: bool = True):

        yaml_dict = get_yaml(filepath)
        assert all(
            p in yaml_dict
            for p in ["classname", "associations", "dataset", "quality", "storage"]
        )

        # dataset_model = cls.resolve_dataset_configurations(
        #     v=yaml_dict["dataset"], values=yaml_dict
        # )
        # quality_model = cls.resolve_quality_configurations(
        #     v=yaml_dict["quality"], values=yaml_dict
        # )
        # storage_model = cls.resolve_storage_configurations(
        #     v=yaml_dict["storage"], values=yaml_dict
        # )

        # yaml_dict["dataset"] = dataset_model
        # yaml_dict["quality"] = quality_model
        # yaml_dict["storage"] = storage_model

        if not validate:
            return cls.construct(**yaml_dict)

        return cls(**yaml_dict)

    def instaniate_pipeline(self, validate: bool = True) -> Any:
        # TEST: Pipeline instantiation from config file needs to be tested
        return recusive_instantiate(self, validate=validate)
