# file core.pxd

cimport cdsproc3

cdef class PyVarTarget:
    cdef cdsproc3.VarTarget *cobj
    cdef set_vartarget(self, cdsproc3.VarTarget *obj)

cdef class PyProcLoc:
    cdef cdsproc3.ProcLoc *cobj
    cdef set_procloc(self, cdsproc3.ProcLoc *obj)

