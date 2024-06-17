from .parameterized_class import ParameterizedClass
from .standards_type import StandardsType

from .assign_data import assign_data
from .datetime_substitutions import datetime_substitutions
from .decode_cf import decode_cf
from .generate_schema import generate_schema
from .get_datastream import get_datastream
from .get_fields_from_dataset import get_fields_from_dataset
from .get_fields_from_datastream import get_fields_from_datastream
from .get_filename import get_filename
from .get_file_datetime_str import get_file_datetime_str
from .get_start_date_and_time_str import get_start_date_and_time_str
from .get_start_time import get_start_time
from .model_to_dict import model_to_dict
from .record_corrections_applied import record_corrections_applied
from ._nested_union import _nested_union

from tsdat.const import DATASTREAM_TEMPLATE, FILENAME_TEMPLATE

__all__ = [
    "ParameterizedClass",
    "assign_data",
    "decode_cf",
    "generate_schema",
    "get_datastream",
    "get_fields_from_datastream",
    "get_filename",
    "get_start_date_and_time_str",
    "get_start_time",
    "record_corrections_applied",
    "DATASTREAM_TEMPLATE",
    "FILENAME_TEMPLATE",
]

# IDEA: Method to print a summary of the list of problems with the data
