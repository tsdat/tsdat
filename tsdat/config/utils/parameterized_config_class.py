from typing import (
    Any,
    Dict,
)

from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    BaseModel,
    Extra,
    Field,
    StrictStr,
    validator,
)
from pydantic.utils import import_string


class ParameterizedConfigClass(BaseModel, extra=Extra.forbid):
    # Unfortunately, the classname has to be a string type unless PyObject becomes JSON
    # serializable: https://github.com/samuelcolvin/pydantic/discussions/3842
    classname: StrictStr = Field(
        description=(
            "The import path to the Python class that should be used, e.g., if"
            " your import statement looks like `from foo.bar import Baz`, then your"
            " classname would be `foo.bar.Baz`."
        ),
    )
    parameters: Dict[str, Any] = Field(
        {},
        description=(
            "Optional dictionary that will be passed to the Python class specified by"
            " 'classname' when it is instantiated. If the object is a tsdat class, then"
            " the parameters will typically be made accessible under the `params`"
            " property on an instance of the class. See the documentation for"
            " individual classes for more information."
        ),
    )

    @validator("classname")
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
        params = {field: getattr(self, field) for field in self.__fields_set__}
        _cls = import_string(params.pop("classname"))
        return _cls(**params)
