import yaml
from jsonpointer import set_pointer
from pathlib import Path
from pydantic import BaseModel, Extra, Field, StrictStr, validator, FilePath
from pydantic.utils import import_string
from pydantic.generics import GenericModel
from typing import Any, cast, Dict, Generic, List, Protocol, Sequence, Set, TypeVar

__all__ = [
    "YamlModel",
    "OverrideableModel",
    "ParametrizedClass",
    "find_duplicates",
    "get_yaml",
    "recusive_instantiate",
    "Definition",
]


class YamlModel(BaseModel):
    @classmethod
    def from_yaml(cls, filepath: Path, validate: bool = True):
        with open(filepath, "r") as _config:
            config = list(yaml.safe_load_all(_config))[0]
            if not validate:
                return cls.construct(**config)
            return cls(**config)

    @classmethod
    def generate_schema(cls, output_file: Path):
        with open(output_file, "w") as schemafile:
            schemafile.write(cls.schema_json(indent=4))

    @classmethod
    def from_override(
        cls, filepath: Path, overrides: Dict[str, Any], validate: bool = True
    ):
        base_dict = list(yaml.safe_load_all(filepath.read_text()))[0]
        for pointer, new_value in overrides.items():
            set_pointer(base_dict, pointer, new_value)
        if not validate:
            return cls.construct(**base_dict)
        return cls(**base_dict)


Definition = TypeVar("Definition", bound=BaseModel)


class OverrideableModel(
    YamlModel, GenericModel, Generic[Definition], extra=Extra.forbid
):
    path: FilePath
    overrides: Dict[str, Any] = dict()

    def get_defaults_dict(self) -> Dict[Any, Any]:
        txt = self.path.read_text()
        return list(yaml.safe_load_all(txt))[0]

    def get_new_config(self) -> Dict[Any, Any]:
        defaults = self.get_defaults_dict()
        for pointer, new_value in self.overrides.items():
            set_pointer(defaults, pointer, new_value)
        return defaults


class ParametrizedClass(BaseModel, extra=Extra.forbid):
    # Unfortunately, the classname has to be a string type unless PyObject becomes JSON
    # serializable: https://github.com/samuelcolvin/pydantic/discussions/3842
    classname: StrictStr = Field(
        description="The module path to the Python class that should be used, e.g., if"
        " you would write in your script `from tsdat.config.utils.converters import"
        " DefaultConverter` then you would put"
        " `tsdat.config.utils.converters.DefaultConverter` as the classname.",
    )
    parameters: Dict[StrictStr, Any] = Field(
        dict(),
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

    def instantiate(self, validate: bool = True) -> Any:
        _cls = import_string(self.classname)
        params: Dict[str, Any] = self.dict(exclude={"classname"})
        if not validate:
            return _cls.construct(**params)
        return _cls(**params)


def recusive_instantiate(model: Any, validate: bool = True) -> Any:
    """---------------------------------------------------------------------------------
    Recursively calls model.instantiate() on all ParametrizedClass instances under the
    the model, resulting in a new model which follows the same general structure as the
    given model, but possibly containing totally different properties and methods.

    Args:
        model (Any): The object to recursively instantiate.
        validate (bool, optional): Validate the instantiated object. Defaults to True.

    Returns:
        Any: The recusively-instantiated object.

    ---------------------------------------------------------------------------------"""
    # Case: ParametrizedClass. Want to instantiate any sub-models then return the class
    # with all submodels recusively instantiated, then statically instantiate the model.
    # Note: the model is instantiated last so that sub-models are only processed once.
    if isinstance(model, ParametrizedClass):
        fields = model.__fields_set__ - {"classname"}  # No point checking classname
        for field in fields:
            setattr(model, field, recusive_instantiate(getattr(model, field)))
        return model.instantiate(validate=validate)

    # Case: BaseModel. Want to instantiate any sub-models then return the model itself.
    elif isinstance(model, BaseModel):
        fields = model.__fields_set__
        assert "classname" not in fields
        for field in fields:
            setattr(model, field, recusive_instantiate(getattr(model, field)))
        return model

    # Case: List. Want to iterate through and recursively instantiate all sub-models in
    # the list, then return everything as a list.
    elif isinstance(model, List):
        return [recusive_instantiate(m) for m in cast(List[Any], model)]

    # Case Dict. Want to iterate through and recursively instantiate all sub-models in
    # the Dict's values, then return everything as a Dict.
    elif isinstance(model, Dict):
        return {
            k: recusive_instantiate(v) for k, v in cast(Dict[str, Any], model).items()
        }

    # Base case: Anything else; just return the value
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


def get_yaml(filepath: Path) -> Dict[Any, Any]:
    return list(yaml.safe_load_all(filepath.read_text()))[0]
