from pydantic import (
    Field,
)

from .dataset_config import DatasetConfig
from ..attributes import ACDDGlobalAttrs


class ACDDDatasetConfig(DatasetConfig):
    attrs: ACDDGlobalAttrs = Field(
        ...,
        description=(
            "Attributes that pertain to the dataset as a whole (as opposed to"
            " attributes that are specific to individual variables."
        ),
    )
