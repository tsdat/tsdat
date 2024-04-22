# IDEA: Variables/Coordinates via __root__=Dict[str, Variable/Coordinate]
# TODO: Variables/Coordinates validators; name uniqueness, coords has time, etc

from .coordinate import Coordinate
from .variable import Variable
from .variable_attributes import VariableAttributes

from .ureg import ureg

__all__ = [
    "Coordinate",
    "Variable",
    "VariableAttributes",
]
