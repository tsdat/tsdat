from typing import (
    Any,
    Dict,
    List,
    cast,
)

from jsonpointer import set_pointer  # type: ignore
from pydantic import (
    BaseModel,
)
from pydantic.utils import import_string

from .parameterized_config_class import ParameterizedConfigClass


def recursive_instantiate(model: Any) -> Any:
    """---------------------------------------------------------------------------------
    Instantiates all ParametrizedClass components and subcomponents of a given model.

    Recursively calls model.instantiate() on all ParameterizedConfigClass instances under
    the model, resulting in a new model which follows the same general structure as
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
