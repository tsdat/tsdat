from pathlib import Path
from pydantic import (
    BaseModel,
    BaseSettings,
    Extra,
)
from typing import Any, Dict, List, Optional, Pattern, Protocol
from .dataset import DatasetDefinition
from .quality import QualityDefinition
from .storage import StorageDefinition
from .utils import ParametrizedClass, YamlModel, OverrideableModel, get_yaml


class PipelineSettings(BaseSettings, extra=Extra.allow):
    # FIXME: Add more settings that someone might want to configure here
    persist_inputs: bool = True
    delete_inputs: bool = False


class Config(BaseModel, extra=Extra.forbid):
    dataset: DatasetDefinition
    quality: QualityDefinition
    storage: StorageDefinition
    settings: PipelineSettings = PipelineSettings()

    @classmethod
    def from_yaml(
        cls,
        dataset_path: Path,
        storage_path: Path,
        quality_path: Path,
        validate: bool = True,
        **settings: Any
    ):
        ds_def = DatasetDefinition(**get_yaml(dataset_path))
        qc_def = QualityDefinition(**get_yaml(quality_path))
        st_def = StorageDefinition(**get_yaml(storage_path))
        p_set = PipelineSettings(**settings)

        values: Dict[str, Any] = dict(
            dataset=ds_def,
            quality=qc_def,
            storage=st_def,
            settings=p_set,
        )

        if not validate:
            return Config(**values)
        return Config(**values)

    # TODO: Validate exclude and apply_to parameters in quality config managers are all
    # coordinate or variable names


class Config_(BaseModel, extra=Extra.forbid):
    dataset: OverrideableModel[DatasetDefinition]
    quality: OverrideableModel[QualityDefinition]
    storage: OverrideableModel[StorageDefinition]
    settings: PipelineSettings = PipelineSettings()

    def merge_overrides(self, validate: bool = True) -> Config:
        ds = self.dataset.get_new_config()
        qc = self.quality.get_new_config()
        st = self.storage.get_new_config()
        ps = self.settings

        values: Dict[str, Any] = dict(dataset=ds, quality=qc, storage=st, settings=ps)

        if not validate:
            return Config.construct(**values)
        return Config(**values)


class Pipeline(Protocol):
    @property
    def config(self) -> Config:
        ...

    @property
    def storage(self) -> Any:
        ...

    def run(self, inputs: Any) -> Any:
        ...


class PipelineDefinition(ParametrizedClass, YamlModel, extra=Extra.allow):
    # Strategy: This will be used to model the pipeline config file

    associations: Optional[List[Pattern]]  # type: ignore
    config: Config_
    settings: PipelineSettings = PipelineSettings()

    # FIXME: This method instantiates a config object twice, which is not optimal
    def instantiate(self, validate: bool = True, **extra_settings: Any) -> Pipeline:
        cls = self.get_cls()
        config = self.config.merge_overrides(validate=validate)

        for key, value in extra_settings.items():
            setattr(config.settings, key, value)

        return cls(config=config, storage=config.storage.instantiate())
