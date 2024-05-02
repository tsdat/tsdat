from typing import (
    Any,
    Dict,
    Generic,
)
from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    Extra,
    Field,
    FilePath,
)
from pydantic.generics import GenericModel

from .config import Config
from .yaml_model import YamlModel


class Overrideable(YamlModel, GenericModel, Generic[Config], extra=Extra.forbid):
    path: FilePath = Field(
        description=(
            "Path to the configuration file to borrow configurations from.\nNote that"
            " this path is relative to the project root, so you should include any"
            " paths in between the project root and your config file.\nE.g.,"
            " `pipelines/lidar/config/dataset.yaml`"
        )
    )

    overrides: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Overrides to apply to the config file referenced by `path`.\nOverrides are"
            " defined in `key`: `value` pairs, where the `key` is a pointer to the"
            " object in the config file to override and the `value` is what should"
            " replace it.\nThe format of the keys is a cross between path-like"
            " structures and a python dictionary. For example, to change the"
            " 'location_id' property on the python object `obj = {'attrs':"
            " {'location_id': 'abc'}, 'data_vars': {...}}` to 'sgp' you would write"
            " `/attrs/location_id: 'sgp'`.\nOverrides are implemented using"
            " https://python-json-pointer.readthedocs.io/en/latest/tutorial.html"
        ),
    )
