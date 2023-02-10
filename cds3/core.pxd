# file cds3.pxd

from ccds3 cimport *
cimport numpy as np

cdef class Object:
    cdef CDSObject *cds_object
    cdef set_object(self, CDSObject *obj)

cdef class Group(Object):
    cdef CDSGroup *c_ob
    cdef set_group(self, CDSGroup *obj)

cdef class Dim(Object):
    cdef CDSDim *c_ob
    cdef set_dim(self, CDSDim *obj)

cdef class Att(Object):
    cdef CDSAtt *c_ob
    cdef set_att(self, CDSAtt *obj)

cdef class Var(Object):
    cdef CDSVar *c_ob
    cdef set_var(self, CDSVar *obj)
    cpdef np.ndarray get_datap(self, size_t sample_start=*)

cdef class VarGroup(Object):
    cdef CDSVarGroup *c_ob
    cdef set_vargroup(self, CDSVarGroup *obj)

cdef class VarArray(Object):
    cdef CDSVarArray *c_ob
    cdef set_vararray(self, CDSVarArray *obj)
