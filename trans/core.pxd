from cds3.ccds3 cimport *
from cds3.core cimport *

# Map to corresponding C function so we can call it from our python method
cdef extern from "trans.h" nogil:
    int cds_transform_driver(
            CDSVar *invar,
            CDSVar *qc_invar,
            CDSVar *outvar,
            CDSVar *qc_outvar
    )