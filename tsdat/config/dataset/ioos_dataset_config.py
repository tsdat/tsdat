from pydantic import (
    Field,
)

from .dataset_config import DatasetConfig
from ..attributes import IOOSGlobalAttrs


class IOOSDatasetConfig(DatasetConfig):
    attrs: IOOSGlobalAttrs = Field(
        description=(
            "Attributes that pertain to the dataset as a whole (as opposed to"
            " attributes that are specific to individual variables."
        ),
    )
