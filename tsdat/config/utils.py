import yaml
from jsonpointer import set_pointer
from pathlib import Path
from pydantic import BaseModel, Extra, Field, StrictStr, validator, FilePath
from pydantic.utils import import_string
from pydantic.generics import GenericModel
from typing import Any, Dict, Generic, List, Protocol, Sequence, Set, TypeVar

__all__ = [
    "OverrideableModel",
    "YamlModel",
    "ParametrizedClass",
    "find_duplicates",
    "get_yaml",
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

    # NOTE: Attaching the instantiated object to the parametrized class model does not
    # work; the objects are not JSON serializable and the code crashes immediately. I
    # settled on adding an `instantiate()` method which returns the instantiated object.
    def instantiate(self) -> Any:
        _cls = self.get_cls()
        return _cls(parameters=self.parameters)

    def get_cls(self) -> Any:
        """Wrapper around `pydantic.utils.import_string(dotted_path)`. Import a dotted
        module path and return the attribute/class designated by the last name in the
        path. Raise ImportError if the import fails."""
        return import_string(self.classname)


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
