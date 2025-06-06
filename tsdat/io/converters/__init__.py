# TODO:
#  IDEA: Implement MultiDimensionalGrouper to group collection of 1D variables into a 2D
#  variable. (will need a better name)
#  IDEA: Use the flyweight pattern to limit memory usage if identical converters would
#  be created.
#  IDEA: "@data_converter()" decorator so DataConverters can be defined as functions in
#  user code. Arguments to data_converter can be parameters to the class.

from .nearest_neighbor import NearestNeighbor
from .string_to_datetime import StringToDatetime
from .units_converter import UnitsConverter

__all__ = [
    "NearestNeighbor",
    "StringToDatetime",
    "UnitsConverter",
]
