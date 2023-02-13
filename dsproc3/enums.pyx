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
#******************************************************************************

import sys

# Convert C string to Python string, depending on which version runs
def _to_python_string(c_string):
    if sys.version_info[0] < 3:
        # In Python 2, strings are byte strings, just like the C strings
        return c_string
    else:
        # In Python 3, strings are Unicode
        return _to_unicode(c_string)

#Convert all C strings into python3 (unicode) strings
# I think this is the way to go
# Will double check this
cdef unicode _to_unicode(char *x):
    return x.decode('UTF-8', 'strict')

cimport cdsproc3_enums

DSP_RETRIEVER = cdsproc3_enums.DSP_RETRIEVER
DSP_RETRIEVER_REQUIRED = cdsproc3_enums.DSP_RETRIEVER_REQUIRED
DSP_TRANSFORM = cdsproc3_enums.DSP_TRANSFORM
DSP_INGEST = cdsproc3_enums.DSP_INGEST

PM_GENERIC = cdsproc3_enums.PM_GENERIC
PM_RETRIEVER_VAP = cdsproc3_enums.PM_RETRIEVER_VAP
PM_TRANSFORM_VAP = cdsproc3_enums.PM_TRANSFORM_VAP
PM_INGEST = cdsproc3_enums.PM_INGEST
PM_RETRIEVER_INGEST = cdsproc3_enums.PM_RETRIEVER_INGEST
PM_TRANSFORM_INGEST = cdsproc3_enums.PM_TRANSFORM_INGEST

DSR_INPUT = cdsproc3_enums.DSR_INPUT
DSR_OUTPUT = cdsproc3_enums.DSR_OUTPUT

SPLIT_ON_STORE = cdsproc3_enums.SPLIT_ON_STORE
SPLIT_ON_HOURS = cdsproc3_enums.SPLIT_ON_HOURS
SPLIT_ON_DAYS = cdsproc3_enums.SPLIT_ON_DAYS
SPLIT_ON_MONTHS = cdsproc3_enums.SPLIT_ON_MONTHS

VAR_SKIP_TRANSFORM = cdsproc3_enums.VAR_SKIP_TRANSFORM

SUCCESS = _to_python_string(cdsproc3_enums.DSPROC_SUCCESS)
ENOMEM = _to_python_string(cdsproc3_enums.DSPROC_ENOMEM)
ENODATA = _to_python_string(cdsproc3_enums.DSPROC_ENODATA)
EINITSIGS = _to_python_string(cdsproc3_enums.DSPROC_EINITSIGS)
ERUNTIME = _to_python_string(cdsproc3_enums.DSPROC_ERUNTIME)
EDSPATH = _to_python_string(cdsproc3_enums.DSPROC_EDSPATH)
ELOGSPATH = _to_python_string(cdsproc3_enums.DSPROC_ELOGSPATH)
ELOGOPEN = _to_python_string(cdsproc3_enums.DSPROC_ELOGOPEN)
EPROVOPEN = _to_python_string(cdsproc3_enums.DSPROC_EPROVOPEN)
EMAILINIT = _to_python_string(cdsproc3_enums.DSPROC_EMAILINIT)
EDBERROR = _to_python_string(cdsproc3_enums.DSPROC_EDBERROR)
EDBCONNECT = _to_python_string(cdsproc3_enums.DSPROC_EDBCONNECT)
EDQRDBERROR = _to_python_string(cdsproc3_enums.DSPROC_EDQRDBERROR)
EDQRDBCONNECT = _to_python_string(cdsproc3_enums.DSPROC_EDQRDBCONNECT)
EDIRLIST = _to_python_string(cdsproc3_enums.DSPROC_EDIRLIST)
EBADINDSC = _to_python_string(cdsproc3_enums.DSPROC_EBADINDSC)
EBADOUTDSC = _to_python_string(cdsproc3_enums.DSPROC_EBADOUTDSC)
EBADDSID = _to_python_string(cdsproc3_enums.DSPROC_EBADDSID)
EMINTIME = _to_python_string(cdsproc3_enums.DSPROC_EMINTIME)
EFUTURETIME = _to_python_string(cdsproc3_enums.DSPROC_EFUTURETIME)
ETIMEORDER = _to_python_string(cdsproc3_enums.DSPROC_ETIMEORDER)
ETIMEOVERLAP = _to_python_string(cdsproc3_enums.DSPROC_ETIMEOVERLAP)
EFILECOPY = _to_python_string(cdsproc3_enums.DSPROC_EFILECOPY)
EFILEMOVE = _to_python_string(cdsproc3_enums.DSPROC_EFILEMOVE)
EFILEOPEN = _to_python_string(cdsproc3_enums.DSPROC_EFILEOPEN)
EFILESTATS = _to_python_string(cdsproc3_enums.DSPROC_EFILESTATS)
EUNLINK = _to_python_string(cdsproc3_enums.DSPROC_EUNLINK)
ENOSRCFILE = _to_python_string(cdsproc3_enums.DSPROC_ENOSRCFILE)
ENOFILETIME = _to_python_string(cdsproc3_enums.DSPROC_ENOFILETIME)
EDESTDIRMAKE = _to_python_string(cdsproc3_enums.DSPROC_EDESTDIRMAKE)
ETIMECALC = _to_python_string(cdsproc3_enums.DSPROC_ETIMECALC)
EFILEMD5 = _to_python_string(cdsproc3_enums.DSPROC_EFILEMD5)
EMD5CHECK = _to_python_string(cdsproc3_enums.DSPROC_EMD5CHECK)
ECDSALLOCVAR = _to_python_string(cdsproc3_enums.DSPROC_ECDSALLOCVAR)
ECDSCOPYVAR = _to_python_string(cdsproc3_enums.DSPROC_ECDSCOPYVAR)
ECLONEVAR = _to_python_string(cdsproc3_enums.DSPROC_ECLONEVAR)
ECDSDEFVAR = _to_python_string(cdsproc3_enums.DSPROC_ECDSDEFVAR)
ECDSDELVAR = _to_python_string(cdsproc3_enums.DSPROC_ECDSDELVAR)
ECDSCOPY = _to_python_string(cdsproc3_enums.DSPROC_ECDSCOPY)
ECDSCHANGEATT = _to_python_string(cdsproc3_enums.DSPROC_ECDSCHANGEATT)
ECDSSETATT = _to_python_string(cdsproc3_enums.DSPROC_ECDSSETATT)
ECDSSETDIM = _to_python_string(cdsproc3_enums.DSPROC_ECDSSETDIM)
ECDSSETDATA = _to_python_string(cdsproc3_enums.DSPROC_ECDSSETDATA)
ECDSSETTIME = _to_python_string(cdsproc3_enums.DSPROC_ECDSSETTIME)
ECDSGETTIME = _to_python_string(cdsproc3_enums.DSPROC_ECDSGETTIME)
ENODOD = _to_python_string(cdsproc3_enums.DSPROC_ENODOD)
ENORETRIEVER = _to_python_string(cdsproc3_enums.DSPROC_ENORETRIEVER)
EBADRETRIEVER = _to_python_string(cdsproc3_enums.DSPROC_EBADRETRIEVER)
EREQVAR = _to_python_string(cdsproc3_enums.DSPROC_EREQVAR)
ERETRIEVER = _to_python_string(cdsproc3_enums.DSPROC_ERETRIEVER)
ENCOPEN = _to_python_string(cdsproc3_enums.DSPROC_ENCOPEN)
ENCREAD = _to_python_string(cdsproc3_enums.DSPROC_ENCREAD)
ENOTRANSFORM = _to_python_string(cdsproc3_enums.DSPROC_ENOTRANSFORM)
ETRANSFORM = _to_python_string(cdsproc3_enums.DSPROC_ETRANSFORM)
ETRANSPARAMLOAD = _to_python_string(cdsproc3_enums.DSPROC_ETRANSPARAMLOAD)
EVARMAP = _to_python_string(cdsproc3_enums.DSPROC_EVARMAP)

LIB_NAME = _to_python_string(cdsproc3_enums.DSPROC_LIB_NAME)

DS_STANDARD_QC = cdsproc3_enums.DS_STANDARD_QC
DS_FILTER_NANS = cdsproc3_enums.DS_FILTER_NANS
DS_OVERLAP_CHECK = cdsproc3_enums.DS_OVERLAP_CHECK
DS_PRESERVE_OBS = cdsproc3_enums.DS_PRESERVE_OBS
DS_DISABLE_MERGE = cdsproc3_enums.DS_DISABLE_MERGE
DS_SKIP_TRANSFORM = cdsproc3_enums.DS_SKIP_TRANSFORM
DS_ROLLUP_TRANS_QC = cdsproc3_enums.DS_ROLLUP_TRANS_QC
DS_SCAN_MODE = cdsproc3_enums.DS_SCAN_MODE
DS_OBS_LOOP = cdsproc3_enums.DS_OBS_LOOP
QUICKLOOK_ONLY = cdsproc3_enums.QUICKLOOK_ONLY
QUICKLOOK_DISABLE = cdsproc3_enums.QUICKLOOK_DISABLE

DSF_NETCDF = cdsproc3_enums.DSF_NETCDF
DSF_RAW = cdsproc3_enums.DSF_RAW
DSF_PNG = cdsproc3_enums.DSF_PNG
DSF_JPG = cdsproc3_enums.DSF_JPG
