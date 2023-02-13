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

cdef extern from "dsproc3.h" nogil:
    enum:
        DSP_RETRIEVER
        DSP_RETRIEVER_REQUIRED
        DSP_TRANSFORM
        DSP_INGEST

    ctypedef enum ProcModel:
        PM_GENERIC
        PM_RETRIEVER_VAP
        PM_TRANSFORM_VAP
        PM_INGEST
        PM_RETRIEVER_INGEST
        PM_TRANSFORM_INGEST

    ctypedef enum DSRole:
        DSR_INPUT
        DSR_OUTPUT

    ctypedef enum SplitMode:
        SPLIT_ON_STORE
        SPLIT_ON_HOURS
        SPLIT_ON_DAYS
        SPLIT_ON_MONTHS

    enum:
        VAR_SKIP_TRANSFORM

    char* DSPROC_SUCCESS
    char* DSPROC_ENOMEM
    char* DSPROC_ENODATA
    char* DSPROC_EINITSIGS
    char* DSPROC_ERUNTIME
    char* DSPROC_EDSPATH
    char* DSPROC_ELOGSPATH
    char* DSPROC_ELOGOPEN
    char* DSPROC_EPROVOPEN
    char* DSPROC_EMAILINIT
    char* DSPROC_EDBERROR
    char* DSPROC_EDBCONNECT
    char* DSPROC_EDQRDBERROR
    char* DSPROC_EDQRDBCONNECT
    char* DSPROC_EDIRLIST
    char* DSPROC_EBADINDSC
    char* DSPROC_EBADOUTDSC
    char* DSPROC_EBADDSID
    char* DSPROC_EMINTIME
    char* DSPROC_EFUTURETIME
    char* DSPROC_ETIMEORDER
    char* DSPROC_ETIMEOVERLAP
    char* DSPROC_EFILECOPY
    char* DSPROC_EFILEMOVE
    char* DSPROC_EFILEOPEN
    char* DSPROC_EFILESTATS
    char* DSPROC_EUNLINK
    char* DSPROC_ENOSRCFILE
    char* DSPROC_ENOFILETIME
    char* DSPROC_EDESTDIRMAKE
    char* DSPROC_ETIMECALC
    char* DSPROC_EFILEMD5
    char* DSPROC_EMD5CHECK
    char* DSPROC_ECDSALLOCVAR
    char* DSPROC_ECDSCOPYVAR
    char* DSPROC_ECLONEVAR
    char* DSPROC_ECDSDEFVAR
    char* DSPROC_ECDSDELVAR
    char* DSPROC_ECDSCOPY
    char* DSPROC_ECDSCHANGEATT
    char* DSPROC_ECDSSETATT
    char* DSPROC_ECDSSETDIM
    char* DSPROC_ECDSSETDATA
    char* DSPROC_ECDSSETTIME
    char* DSPROC_ECDSGETTIME
    char* DSPROC_ENODOD
    char* DSPROC_ENORETRIEVER
    char* DSPROC_EBADRETRIEVER
    char* DSPROC_EREQVAR
    char* DSPROC_ERETRIEVER
    char* DSPROC_ENCOPEN
    char* DSPROC_ENCREAD
    char* DSPROC_ENOTRANSFORM
    char* DSPROC_ETRANSFORM
    char* DSPROC_ETRANSPARAMLOAD
    char* DSPROC_EVARMAP

    char* DSPROC_LIB_NAME

    enum:
        DS_STANDARD_QC
        DS_FILTER_NANS
        DS_OVERLAP_CHECK
        DS_PRESERVE_OBS
        DS_DISABLE_MERGE
        DS_SKIP_TRANSFORM
        DS_ROLLUP_TRANS_QC
        DS_SCAN_MODE
        DS_OBS_LOOP
        QUICKLOOK_ONLY
        QUICKLOOK_DISABLE

    ctypedef enum DSFormat:
        DSF_NETCDF
        DSF_RAW
        DSF_PNG
        DSF_JPG
