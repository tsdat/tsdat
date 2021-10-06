import importlib
from typing import Dict, List, Tuple, Union
import yaml
import re
import os


def configure_yaml():
    """Configure yaml to automatically substitute environment variables
    referenced by the following syntax: ``${VAR_NAME}``
    """
    path_matcher = re.compile(r"\$\{([^}^{]+)\}")

    def path_constructor(loader, node):
        # Extract the matched value, expand env variable, and replace the match
        value = node.value
        match = path_matcher.match(value)
        env_var = match.group()[2:-1]
        return os.environ.get(env_var, "") + value[match.end() :]

    yaml.add_implicit_resolver("!path", path_matcher)
    yaml.add_constructor("!path", path_constructor)

    yaml.add_implicit_resolver("!path", path_matcher, None, yaml.SafeLoader)
    yaml.add_constructor("!path", path_constructor, yaml.SafeLoader)


def instantiate_handler(
    *args, handler_desc: Dict = None
) -> Union[object, List[object]]:
    """Class to instantiate one or more classes given a dictionary containing
    the path to the class to instantiate and its parameters (optional). This
    method returns the handle(s) to the instantiated class(es).

    :param handler_desc:
        The dictionary containing at least a ``classname`` entry, which should
        be a str that links to a python module on the PYTHONPATH. The
        ``handler_desc`` can also contain a ``parameters`` entry, which will
        is passed as a keyword argument to classes instantiated by this method.
        This parameter defaults to None.
    :type handler_desc: Dict, optional
    :return: The class, or list of classes specified by the handler_desc
    :rtype: Union[object, List[object]]
    """
    handler = None

    if handler_desc:
        classname = handler_desc.get("classname", None)
        params = handler_desc.get("parameters", {})
        handler = _instantiate_class(*args, classname=classname, parameters=params)

    return handler


def _instantiate_class(*args, **kwargs):
    """Instantiates a python class given args and kwargs.

    :return: The python class.
    :rtype: object
    """
    classname = kwargs["classname"]
    parameters = kwargs["parameters"]

    # Convert the class reference to an object
    module_name, class_name = _parse_fully_qualified_name(classname)
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    instance = class_(*args, parameters=parameters)
    return instance


def _parse_fully_qualified_name(fully_qualified_name: str) -> Tuple[str, str]:
    """Splits a fully qualified name into the module name and the class name.

    :param fully_qualified_name: The fully qualified classname.
    :type fully_qualified_name: str
    :return: Returns the module name and class name.
    :rtype: Tuple[str, str]
    """
    module_name, class_name = fully_qualified_name.rsplit(".", 1)
    return module_name, class_name
