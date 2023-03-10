#******************************************************************************
#
#  COPYRIGHT (C) 2012 Battelle Memorial Institute.  All Rights Reserved.
#
#******************************************************************************
#
#  Author:
#     name:  Jeff Daily
#     phone: (509) 372-6548
#     email: jeff.daily@pnnl.gov
#
#******************************************************************************

cdef extern from "cds3.h" nogil:
    char* CDS_LIB_NAME

    enum:
        CDS_SKIP_DIMS
        CDS_SKIP_GROUP_ATTS
        CDS_SKIP_VAR_ATTS
        CDS_SKIP_VARS
        CDS_SKIP_DATA
        CDS_SKIP_SUBGROUPS
        CDS_PRINT_VARGROUPS
        CDS_COPY_LOCKS
        CDS_EXCLUSIVE
        CDS_OVERWRITE_DIMS
        CDS_OVERWRITE_ATTS
        CDS_OVERWRITE_DATA
        CDS_OVERWRITE

    # default _FillValues used by the NetCDF library (see netcdf.h)
    char        CDS_FILL_CHAR
    signed char CDS_FILL_BYTE
    short       CDS_FILL_SHORT
    int         CDS_FILL_INT
    long long   CDS_FILL_INT64
    float       CDS_FILL_FLOAT
    double      CDS_FILL_DOUBLE

    # data type ranges used by the NetCDF library (see netcdf.h)
    char        CDS_MAX_CHAR
    char        CDS_MIN_CHAR
    signed char CDS_MAX_BYTE
    signed char CDS_MIN_BYTE
    short       CDS_MAX_SHORT
    short       CDS_MIN_SHORT
    int         CDS_MAX_INT
    int         CDS_MIN_INT
    long long   CDS_MAX_INT64
    long long   CDS_MIN_INT64
    float       CDS_MAX_FLOAT
    float       CDS_MIN_FLOAT
    double      CDS_MAX_DOUBLE
    double      CDS_MIN_DOUBLE

    # Maximum size of a data type
    size_t      CDS_MAX_TYPE_SIZE

    #
    # CDS Object Type.
    #
    ctypedef enum CDSObjectType:
        CDS_GROUP
        CDS_DIM
        CDS_ATT
        CDS_VAR
        CDS_VARGROUP
        CDS_VARARRAY

    #
    # CDS Data Types.
    #
    ctypedef enum CDSDataType:
        CDS_NAT
        CDS_CHAR
        CDS_BYTE
        CDS_SHORT
        CDS_INT
        CDS_INT64
        CDS_FLOAT
        CDS_DOUBLE

    enum:
        CDS_EQ
        CDS_LT
        CDS_GT
        CDS_LTEQ
        CDS_GTEQ

    enum:
        CDS_IGNORE_UNITS
        CDS_DELTA_UNITS
