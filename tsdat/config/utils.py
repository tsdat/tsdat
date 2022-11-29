import os
import yaml
import warnings
from jsonpointer import set_pointer  # type: ignore
from dunamai import Style, Version
from pathlib import Path
from pydantic import BaseModel, Extra, Field, StrictStr, validator, FilePath
from pydantic.utils import import_string
from pydantic.generics import GenericModel
from typing import (
    Any,
    Optional,
    cast,
    Dict,
    Generic,
    List,
    Protocol,
    Sequence,
    Set,
    TypeVar,
)


__all__ = [
    "ParameterizedConfigClass",
    "Overrideable",
    "recursive_instantiate",
    "read_yaml",
    "get_code_version",
    "YamlModel",
]


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

        ------------------------------------------------------------------------------------"""
        config = read_yaml(filepath)
        if overrides:
            for pointer, new_value in overrides.items():
                set_pointer(config, pointer, new_value)
        return cls(**config)

    @classmethod
    def generate_schema(cls, output_file: Path):
        """------------------------------------------------------------------------------------
        Generates JSON schema from the model fields and type annotations.

        Args:
            output_file (Path): The path to store the JSON schema.

        ------------------------------------------------------------------------------------"""
        output_file.write_text(cls.schema_json(indent=4))


Config = TypeVar("Config", bound=BaseModel)


class Overrideable(YamlModel, GenericModel, Generic[Config], extra=Extra.forbid):
    path: FilePath = Field(
        description="Path to the configuration file to borrow configurations from.\n"
        "Note that this path is relative to the project root, so you should include any"
        " paths in between the project root and your config file.\n"
        "E.g., `pipelines/lidar/config/dataset.yaml`"
    )

    overrides: Dict[str, Any] = Field(
        default_factory=dict,
        description="Overrides to apply to the config file referenced by `path`.\n"
        "Overrides are defined in `key`: `value` pairs, where the `key` is a pointer to"
        " the object in the config file to override and the `value` is what should"
        " replace it.\n"
        "The format of the keys is a cross between path-like structures and a python"
        " dictionary. For example, to change the 'location_id' property on the python"
        " object `obj = {'attrs': {'location_id': 'abc'}, 'data_vars': {...}}` to 'sgp'"
        " you would write `/attrs/location_id: 'sgp'`.\n"
        "Overrides are implemented using https://python-json-pointer.readthedocs.io/en/latest/tutorial.html",
    )

    # def get_defaults_dict(self) -> Dict[Any, Any]:
    #     txt = self.path.read_text()
    #     return list(yaml.safe_load_all(txt))[0]

    # def merge_overrides(self) -> Dict[Any, Any]:
    #     defaults = self.get_defaults_dict()
    #     for pointer, new_value in self.overrides.items():
    #         set_pointer(defaults, pointer, new_value)
    #     return defaults


def matches_overrideable_schema(model_dict: Dict[str, Any]):
    return "path" in model_dict


class ParameterizedConfigClass(BaseModel, extra=Extra.forbid):
    # Unfortunately, the classname has to be a string type unless PyObject becomes JSON
    # serializable: https://github.com/samuelcolvin/pydantic/discussions/3842
    classname: StrictStr = Field(
        description="The import path to the Python class that should be used, e.g., if"
        " your import statement looks like `from foo.bar import Baz`, then your"
        " classname would be `foo.bar.Baz`.",
    )
    parameters: Dict[str, Any] = Field(
        {},
        description="Optional dictionary that will be passed to the Python class"
        " specified by 'classname' when it is instantiated. If the object is a tsdat"
        " class, then the parameters will typically be made accessible under the"
        " `params` property on an instance of the class. See the documentation for"
        " individual classes for more information.",
    )

    @validator("classname")
    @classmethod
    def classname_looks_like_a_module(cls, v: StrictStr) -> StrictStr:
        if "." not in v or not v.replace(".", "").replace("_", "").isalnum():
            raise ValueError(f"Classname '{v}' is not a valid classname.")
        return v

    def instantiate(self) -> Any:
        """------------------------------------------------------------------------------------
        Instantiates and returns the class specified by the 'classname' parameter.

        Returns:
            Any: An instance of the specified class.

        ------------------------------------------------------------------------------------"""
        params = {field: getattr(self, field) for field in self.__fields_set__}
        _cls = import_string(params.pop("classname"))
        return _cls(**params)


def recursive_instantiate(model: Any) -> Any:
    """---------------------------------------------------------------------------------
    Instantiates all ParametrizedClass components and subcomponents of a given model.

    Recursively calls model.instantiate() on all ParameterizedConfigClass instances under
    the the model, resulting in a new model which follows the same general structure as
    the given model, but possibly containing totally different properties and methods.

    Note that this method does a depth-first traversal of the model tree to to
    instantiate leaf nodes first. Traversing breadth-first would result in new pydantic
    models attempting to call the __init__ method of child models, which is not valid
    because the child models are ParameterizedConfigClass instances. Traversing
    depth-first allows us to first transform child models into the appropriate type
    using the classname of the ParameterizedConfigClass.

    This method is primarily used to instantiate a Pipeline subclass and all of its
    properties from a yaml pipeline config file, but it can be applied to any other
    pydantic model.

    Args:
        model (Any): The object to recursively instantiate.

    Returns:
        Any: The recursively-instantiated object.

    ---------------------------------------------------------------------------------"""
    # Case: ParameterizedConfigClass. Want to instantiate any sub-models then return the
    # class with all sub-models recursively instantiated, then statically instantiate
    # the model. Note: the model is instantiated last so that sub-models are only
    # processed once.
    if isinstance(model, ParameterizedConfigClass):
        fields = model.__fields_set__ - {"classname"}  # No point checking classname
        for field in fields:
            setattr(model, field, recursive_instantiate(getattr(model, field)))
        model = model.instantiate()

    # Case: BaseModel. Want to instantiate any sub-models then return the model itself.
    elif isinstance(model, BaseModel):
        fields = model.__fields_set__
        if "classname" in fields:
            raise ValueError(
                f"Model '{model.__repr_name__()}' provides a 'classname' but does not"
                " extend ParametrizedConfigClass."
            )
        for field in fields:
            setattr(model, field, recursive_instantiate(getattr(model, field)))

    # Case: List. Want to iterate through and recursively instantiate all sub-models in
    # the list, then return everything as a list.
    elif isinstance(model, List):
        model = [recursive_instantiate(m) for m in cast(List[Any], model)]

    # Case Dict. Want to iterate through and recursively instantiate all sub-models in
    # the Dict's values, then return everything as a Dict, unless the dict is meant to
    # be turned into a parameterized class, in which case we instantiate it as the
    # intended object
    elif isinstance(model, Dict):
        model = {
            k: recursive_instantiate(v) for k, v in cast(Dict[str, Any], model).items()
        }
        if "classname" in model:
            classname: str = model.pop("classname")  # type: ignore
            _cls = import_string(classname)
            return _cls(**model)

    return model


class _NamedClass(Protocol):
    name: str


def find_duplicates(entries: Sequence[_NamedClass]) -> List[str]:
    duplicates: List[str] = []
    seen: Set[str] = set()
    for entry in entries:
        if entry.name in seen:
            duplicates.append(entry.name)
        else:
            seen.add(entry.name)
    return duplicates


def read_yaml(filepath: Path) -> Dict[Any, Any]:
    return list(yaml.safe_load_all(filepath.read_text(encoding="UTF-8")))[0]


def get_code_version() -> str:
    version = "N/A"
    try:
        version = os.environ["CODE_VERSION"]
    except KeyError:
        try:
            version = Version.from_git().serialize(dirty=True, style=Style.SemVer)
        except RuntimeError:
            warnings.warn(
                "Could not get code_version from either the 'CODE_VERSION' environment"
                " variable nor from git history. The 'code_version' global attribute"
                " will be set to 'N/A'.",
                RuntimeWarning,
            )
    return version
