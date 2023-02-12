"""---------------------------------------------------------------------------------------------------------------------
This file is used to add a cython binding for the libtrans cds_tranform_driver method
---------------------------------------------------------------------------------------------------------------------"""

cimport cds3.core


# Implement our binding method in python
def transform_driver(cds3.core.Var invar, cds3.core.Var qc_invar, cds3.core.Var outvar, cds3.core.Var qc_outvar):
    """-----------------------------------------------------------------------------------------------------------------
    Run the transform engine on an input variable, given input QC and an allocated and dimensioned output variable
     (and QC) structure.

    Upon successful output, outvar and qc_outvar will contain the transformed data and QC.

     ** Note that all transformation parameters must have already been applied to the input datastreams associated with
      the invar and/or the coordinate system associated with the outvar.

    Parameters
    ----------
    invar : cds3.core.Var
        pointer to input CDSVar
    qc_invar : cds3.core.Var
        pointer to input QC CDSVar
    outvar : cds3.core.Var
        pointer to output CDSVar; must have dimensions and data spaces allocated, and the dimensions must have coordinate
        variables already created and attached (we use this information to build the output grid to transform to)
    qc_outvar : cds3.core.Var
        pointer to output QC CDSVar; must be dimensioned and allocated as above for outvar

    Returns
    -------
     -  1 if successful
     -  0 if an error occurred - usually deeper in CDS

    """
    cdef CDSVar *cds_invar = invar.c_ob
    cdef CDSVar *cds_qc_invar = qc_invar.c_ob
    cdef CDSVar *cds_outvar = outvar.c_ob
    cdef CDSVar *cds_qc_outvar = qc_outvar.c_ob

    return cds_transform_driver(cds_invar, cds_qc_invar, cds_outvar, cds_qc_outvar)