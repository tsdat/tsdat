#******************************************************************************
#
#  COPYRIGHT (C) 2016 Battelle Memorial Institute.  All Rights Reserved.
#
#******************************************************************************
#
#  Author:
#     name:  Erol Cromwell
#     phone: (509) 372-4648
#     email: erol.cromwell@pnnl.gov
#
#  Notes:
#     The C names are CDS_*. For Python, we don't need the prefix because these
#     symbols are within the "cds3" namespace. This also solves the problem of
#     the C and Python names clashing between the C and Python namespaces that
#     Cython maintains.
#******************************************************************************
import sys

from ccds3_enums cimport *

# Convert C string to Python string, depending on which version runs
def _to_python_string(c_string):
    if sys.version_info[0] < 3:
        # In Python 2, strings are byte strings, just like the C strings
        return c_string
    else:
        # In Python 3, strings are Unicode
        return _to_unicode(c_string)

# Convert C string (byte string) to Python 3 string( unicode string)
cdef unicode _to_unicode(char *x):
    return x.decode('UTF-8', 'strict')

LIB_NAME = _to_python_string( CDS_LIB_NAME )

SKIP_DIMS = CDS_SKIP_DIMS
SKIP_GROUP_ATTS = CDS_SKIP_GROUP_ATTS
SKIP_VAR_ATTS = CDS_SKIP_VAR_ATTS
SKIP_VARS = CDS_SKIP_VARS
SKIP_DATA = CDS_SKIP_DATA
SKIP_SUBGROUPS = CDS_SKIP_SUBGROUPS
PRINT_VARGROUPS = CDS_PRINT_VARGROUPS
COPY_LOCKS = CDS_COPY_LOCKS
EXCLUSIVE = CDS_EXCLUSIVE
OVERWRITE_DIMS = CDS_OVERWRITE_DIMS
OVERWRITE_ATTS = CDS_OVERWRITE_ATTS
OVERWRITE_DATA = CDS_OVERWRITE_DATA
OVERWRITE = CDS_OVERWRITE

# default _FillValues used by the NetCDF library (see netcdf.h)
FILL_CHAR = CDS_FILL_CHAR
FILL_BYTE = CDS_FILL_BYTE
FILL_SHORT = CDS_FILL_SHORT
FILL_INT = CDS_FILL_INT
FILL_INT64 = CDS_FILL_INT64
FILL_FLOAT = CDS_FILL_FLOAT
FILL_DOUBLE = CDS_FILL_DOUBLE

# data type ranges used by the NetCDF library (see netcdf.h)
MAX_CHAR = CDS_MAX_CHAR
MIN_CHAR = CDS_MIN_CHAR
MAX_BYTE = CDS_MAX_BYTE
MIN_BYTE = CDS_MIN_BYTE
MAX_SHORT = CDS_MAX_SHORT
MIN_SHORT = CDS_MIN_SHORT
MAX_INT = CDS_MAX_INT
MIN_INT = CDS_MIN_INT
MAX_INT64 = CDS_MAX_INT64
MIN_INT64 = CDS_MIN_INT64
MAX_FLOAT = CDS_MAX_FLOAT
MIN_FLOAT = CDS_MIN_FLOAT
MAX_DOUBLE = CDS_MAX_DOUBLE
MIN_DOUBLE = CDS_MIN_DOUBLE

# Maximum size of a data type
MAX_TYPE_SIZE = CDS_MAX_TYPE_SIZE

# CDS Object Type.
GROUP = CDS_GROUP
DIM = CDS_DIM
ATT = CDS_ATT
VAR = CDS_VAR
VARGROUP = CDS_VARGROUP
VARARRAY = CDS_VARARRAY

# CDS Data Types.
NAT = CDS_NAT
CHAR = CDS_CHAR
BYTE = CDS_BYTE
SHORT = CDS_SHORT
INT = CDS_INT
INT64 = CDS_INT64
FLOAT = CDS_FLOAT
DOUBLE = CDS_DOUBLE

EQ = CDS_EQ
LT = CDS_LT
GT = CDS_GT
LTEQ = CDS_LTEQ
GTEQ = CDS_GTEQ

IGNORE_UNITS = CDS_IGNORE_UNITS
DELTA_UNITS = CDS_DELTA_UNITS
