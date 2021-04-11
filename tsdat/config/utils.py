import importlib
from typing import Dict
import yaml
import re
import os


def configure_yaml():
    """-------------------------------------------------------------------
    Configure yaml to automatically substitute environment variables
    referenced by the following syntax:  ${VAR_NAME}
    -------------------------------------------------------------------"""
    path_matcher = re.compile(r'\$\{([^}^{]+)\}')

    def path_constructor(loader, node):
        # Extract the matched value, expand env variable, and replace the match
        value = node.value
        match = path_matcher.match(value)
        env_var = match.group()[2:-1]
        return os.environ.get(env_var, '') + value[match.end():]

    yaml.add_implicit_resolver('!path', path_matcher)
    yaml.add_constructor('!path', path_constructor)

    yaml.add_implicit_resolver('!path', path_matcher, None, yaml.SafeLoader)
    yaml.add_constructor('!path', path_constructor, yaml.SafeLoader)


def instantiate_handler(*args, handler_desc=None):
    handler = None

    if handler_desc is not None:
        classname = handler_desc.get('classname', None)
        params = handler_desc.get('parameters', {})

        if classname is None:  # handler is an dictionary of multiple handlers
            handler = []
            for handler_dict in handler_desc.values():
                classname = handler_dict.get('classname', None)
                params = handler_dict.get('parameters', {})
                handler.append(_instantiate_class(*args, classname=classname, parameters=params))

        else:
            handler = _instantiate_class(*args, classname=classname, parameters=params)

    return handler


def _instantiate_class(*args, **kwargs):
    classname = kwargs['classname']
    parameters = kwargs['parameters']

    # Convert the class reference to an object
    module_name, class_name = _parse_fully_qualified_name(classname)
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    instance = class_(*args, parameters=parameters)
    return instance


def _parse_fully_qualified_name(fully_qualified_name: str):
    module_name, class_name = fully_qualified_name.rsplit('.', 1)
    return module_name, class_name
