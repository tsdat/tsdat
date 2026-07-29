import importlib
from typing import Any, Dict
from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator


def _import_string(dotted_path: str) -> Any:
    """Import a class from a dotted module path string."""
    module_path, _, class_name = dotted_path.rpartition(".")
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class ParameterizedConfigClass(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Unfortunately, the classname has to be a string type unless PyObject becomes JSON
    # serializable: https://github.com/samuelcolvin/pydantic/discussions/3842
    classname: StrictStr = Field(
        description=(
            "The import path to the Python class that should be used, e.g., if"
            " your import statement looks like `from foo.bar import Baz`, then your"
            " classname would be `foo.bar.Baz`."
        ),
    )
    """The dotted module path to the pipeline that the specified configurations should
    apply to. To use the built-in IngestPipeline, for example, you would set
    'tsdat.pipeline.pipelines.IngestPipeline' as the classname."""

    parameters: Dict[str, Any] = Field(
        default={},
        description=(
            "Optional dictionary that will be passed to the Python class specified by"
            " 'classname' when it is instantiated. If the object is a tsdat class, then"
            " the parameters will typically be made accessible under the `params`"
            " property on an instance of the class. See the documentation for"
            " individual classes for more information."
        ),
    )

    @field_validator("classname")
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

        ------------------------------------------------------------------------------------
        """
        params = {field: getattr(self, field) for field in self.model_fields_set}
        _cls = _import_string(params.pop("classname"))
        return _cls(**params)
