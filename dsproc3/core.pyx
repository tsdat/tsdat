#cython: embedsignature=True
# file dsproc3.pyx

from __future__ import print_function

from typing import List

import sys
import time
from math import modf

# Cython modules
from libc.stdlib cimport malloc,free
from libc.stdio cimport printf
from cpython.ref cimport PyObject
from cpython.unicode cimport PyUnicode_AsEncodedString
from cpython.pycapsule cimport PyCapsule_GetPointer
#from cpython.pycapsule cimport PyCapsule_Destructor
cdef extern from *:
    int PY_VERSION_HEX
    int PY_MAJOR_VERSION
    int PY_MINOR_VERSION
    char* __FILE__
    int __LINE__

# Import the correct version of the Python string to C string converter
# Warning:  Both functions have to be imported as the same function name, otherwise Cython will break
# when it tries to compile non-imported one (which should not have be reached because of conditionals).  It feels
# pretty hacky because the functions work differently and return different types, but it seems for now to be the way
# to handle Python 2 and 3 in one source
if sys.version_info[0] < 3:
    # Python Major Version 2
    from cpython.string cimport PyString_AsString # Python 2 version of PyBytes_AsString
else:
    # Python Major Version 3
    from cpython.bytes cimport PyBytes_AsString as PyString_AsString #PyBytes_ exlusive to python3

# The stdio.pxd provided by Cython doesn't have this
cdef extern from *:
    ctypedef char const_char "const char"

cdef extern from "Python.h":
    ctypedef void (*PyCapsule_Destructor)(PyObject *)
    PyObject* PyCapsule_New(void *pointer, char *name, 
                            PyCapsule_Destructor destructor)


# our modules
cimport cds3.core
from dsproc3.cdsproc3 cimport *
cimport dsproc3.cdsproc3_enums as enums

# extra C types not made available by Cython
cdef extern from "sys/types.h" nogil:
    ctypedef int time_t

# extra C types from ARM headers
cdef extern from "messenger.h" nogil:
    int msngr_debug_level
    int msngr_provenance_level

# python modules
import inspect
import sys
import codecs

# numpy
import numpy as np
cimport numpy as np
np.import_array()  # initialize the numpy C API

def check_type(x):
    print(type(x))

# Convert python3 string (Unicode string) to C appropriate string 
# (byte string)
# If want to work for python 2.7 as well, will include additional
# definition dependign on python version
def _to_byte_c_string(x):
    return codecs.latin_1_encode(x)[0]

# Allow for bytes or unicode strings, will think on this
#def _to_byte_c_string(x):
#    if type(x) is str: 
#        return codecs.latin_1_encode(x)[0]
#    else: 
#        return x

# Old function
# Convert C string (byte string) to Python 3 string( unicode string)
#def bytes_to_unicode(x):
#    return x.decode('UTF-8', 'strict')
#    #return str(x, 'utf-8')

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

# convenient shorthand used in debug functions
LIB_NAME = enums.DSPROC_LIB_NAME

cdef class PyVarTarget:
    """PyVarTarget"""

    def __cinit__(self):
        """TODO"""
        self.cobj = NULL

    def __init__(self):
        """TODO"""
        pass

    cdef set_vartarget(self, VarTarget *obj):
        """TODO"""
        self.cobj = obj

    def __dealloc__(self):
        """TODO"""
        pass

    property ds_id:
        def __get__(self):
            return self.cobj.ds_id

    property var_name:
        def __get__(self):
            return self.cobj.var_name

cdef class PyProcLoc:
    """PyProcLoc"""

    def __cinit__(self):
        """TODO"""
        self.cobj = NULL

    def __init__(self):
        """TODO"""
        pass

    cdef set_procloc(self, ProcLoc *obj):
        """TODO"""
        self.cobj = obj

    def __dealloc__(self):
        """TODO"""
        pass

    property name:
        def __get__(self):
            return self.cobj.name

    property lat:
        def __get__(self):
            return self.cobj.lat

    property lon:
        def __get__(self):
            return self.cobj.lon

    property alt:
        def __get__(self):
            return self.cobj.alt

#    def get_lat(self):
#       return self.c_ob.lat

cdef inline __line():
    """Returns caller's (filename,lineno,function)."""
    cdef char *filename
    cdef char *function
    cdef int lineno
    frame = inspect.currentframe()
    #if frame.f_back:
    #    frame = frame.f_back
    #if frame.f_back:
    #    frame = frame.f_back
    #if frame.f_back:
    #    frame = frame.f_back

    b_filename = _to_byte_c_string(frame.f_code.co_filename)
    filename = b_filename

    lineno = frame.f_lineno

    b_function = _to_byte_c_string(frame.f_code.co_name)
    function = b_function

    del frame
    return filename, lineno, function

#def LOG(char *format, *args, char *_file="", int _line=0):
def log(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        # Old style string formating (Will eventually be phased out of 
        # Python 3.x )
        try:
            s = format % args
        #New style string formating (python3 )
        except TypeError:
            s = format.format(*args)

    #python byte string of s, equivalent to c string (char *)
    b_s = _to_byte_c_string(s)
    dsproc_log(func, file, line, b_s)


#def WARNING(char *format, *args, char *_file="", int _line=0):
def warning(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_s = _to_byte_c_string(s)
    dsproc_warning(func, file, line, b_s)

#def ERROR(char *status, char *format, *args, char *_file="", int _line=0):
def error(object status, object format, *args):
    """ Generate an error message.

    This function will set the status of the process and append an Error message 
    to the log file and error mail message.

    The status string should be a brief one line error message that will be used 
    as the process status in the database. This is the message that will be 
    displayed in DSView. If the status string is None the error message specified 
    by the format string and args will be used.

    The format string and args will be used to generate the complete and more 
    detailed log and error mail messages. If the format string is None the status 
    string will be used.

    """
    if format == None:  format = status
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_status = _to_byte_c_string(status)
    b_s = _to_byte_c_string(s)
    dsproc_error(func, file, line, b_status, b_s)

def abort(object status, object format, *args):
    """ Abort the process and exit cleanly.

    This function will:

    - set the status of the process
    - append the error message to the log file and error mail message
    - call the users finish_process function (but only if dsproc_abort is not being called from there)
    - exit the process cleanly

    The status string should be a brief one line error message that will be used 
    as the process status in the database. This is the message that will be displayed 
    in DSView. If the status string is None the error message specified by the format 
    string and args will be used.

    The format string and args will be used to generate the complete and more detailed 
    log and error mail messages. If the format string is None the status string will be used.

    Parameters
    ----------

    status: string
        One line error message that will be used as the process status.

    format: string
        Format string, used with args to generate detailed log and error mail messages.

    args: args
        Arguments passed to format parameter to generate detailed log and error mail messages.

    """
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_status = _to_byte_c_string(status)
    b_s = _to_byte_c_string(s)
    dsproc_abort(func, file, line, b_status, b_s)

def mentor_mail(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)
    
    b_s = _to_byte_c_string(s)
    dsproc_mentor_mail(func, file, line, b_s)

#def debug_lv1(char *format, *args, char *_file="", int _line=0):
def debug_lv1(object format, *args):
    s=format
    file,line,func=__line()
    
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)
    
    b_s = _to_byte_c_string(s)
    if msngr_debug_level or msngr_provenance_level:
        dsproc_debug(func, file, line, 1, b_s)

def debug_lv2(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_s = _to_byte_c_string(s)
    if msngr_debug_level or msngr_provenance_level:
        dsproc_debug(func, file, line, 2, b_s)

def debug_lv3(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_s = _to_byte_c_string(s)
    if msngr_debug_level or msngr_provenance_level:
        dsproc_debug(func, file, line, 3, b_s)

def debug_lv4(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_s = _to_byte_c_string(s)
    if msngr_debug_level or msngr_provenance_level:
        dsproc_debug(func, file, line, 4, b_s)

def debug_lv5(object format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    b_s = _to_byte_c_string(s)
    if msngr_debug_level or msngr_provenance_level:
        dsproc_debug(func, file, line, 5, b_s)


##############################################################################
# EC: Functions to convert  Group object into python dictionary-esque item,
#    similar to netCDF4 python module
#    Going to try to design function to be python 2 and 3 friendly
##############################################################################

def _get_group_dict(cds3.core.Group group):
    """Make python dictionary of group

    The dictionary will be in the following form:

    {
        "group_1": {
                    "dimensions": dimension dictionary,
                    "atts": attribute dictionary,
                    "variables" : variable dictionary,
                    "groups" : group dictionary,
                    "ref": cd3.core.Group object for group_1,
                    "parent": cd3.core.Group parent object for group_1
                   },
        "group_2": {
                    "dimensions": dimension dictionary,
                    "atts": attribute dictionary,
                    "variables" : variable dictionary,
                    "groups" : group dictionary,
                    "ref": cd3.core.Group object for group_2,
                    "parent": cd3.core.Group parent object for group_2
                   },
        ...,
        "group_N": {
                    ...
                   }
    }
    
    Parameters
    ----------
    group : cds3.core.Group
        The CDSGroup python object
    
    Returns
    -------
    - Dictionary version of the Group, empty dictionary if no groups.
    
    """

    groups = group.get_groups()

    group_dict = {}

    group_name = group.get_name()

    for g in groups:
        g_dict = {}

        name = g.get_name() # Name of group

        var_dict = _get_var_dict(g)
        if var_dict is None:
            return None
        g_dict['dimensions'] = _get_dim_dict(g)

        # Will come back to path, if relevant
        # g_dict['path'] = "/" + group_name +

        g_dict["parent"] = group
        g_dict["ref"] = g
        g_dict["atts"] = _get_att_dict(g)

        group_dict = _get_group_dict(g)
        if group_dict is None:
            return None
        g_dict["groups"] = group_dict

        group_dict[name] = g_dict

    return group_dict

def _get_var_dict(cds3.core.Group group):
    """Make python dictionary for a Variable

    The dictionary will be in the following form:

    {
        "var_1": {
                    "dimensions": dimension dictionary,
                    "atts": attribute dictionary,
                    "ref"" cd3.core.Var object for var_1
                    "data": pointer to var_1 data
                 },

        "var_2": {
                    "dimensions": dimension dictionary,
                    "atts": attribute dictionary,
                    "ref"" cd3.core.Var object for var_2
                    "data": pointer to var_2 data
                 },
        ...,
        "var_N": {
                    "dimensions": dimension dictionary,
                    "atts": attribute dictionary,
                    "ref"" cd3.core.Var object for var_N
                    "data": pointer to var_N data
                 },
    }
    
    Parameters
    ----------
    group : cds3.core.Group
        The CDSGroup python object
    
    Returns
    -------
    - Dictionary version of the variables, empty dictionary if no
        variables
     """

    gvars = group.get_vars()
    var_dict = {}

    # Number of time samples (assumes time data already created)
    nsamples = 0

    for v in gvars:
        v_dict = {}
        name = v.get_name()

        v_dict["dimensions"] = _get_dim_dict(v)
        v_dict["ref"] = v
        v_dict["atts"] = _get_att_dict(v)

        # Get sample times to see whether need to intialize data or just get data
        if v.sample_count == 0:
            # This is dsproc.init_var_data_index()
            if nsamples == 0:
                time_var = get_time_var(v)
                nsamples = time_var.sample_count

            # Initialize data to missing value
            data = init_var_data_index(v, 0, nsamples, 1)

            # Error initializing data
            if data is None:
                return None

            v_dict["data"] = data

        else:
            # This is dsproc.get_var_data_index()
            v_dict["data"] = get_var_data_index(v)

        var_dict[name] = v_dict


    return var_dict


def _get_dim_dict(cds3.core.Object obj):
    """Make python dictionary of Dimensions

    The dictionary will be in the following form:

    {
        "dim_1": cd3.core.Dim object 1,
        "dim_2": cd3.core.Dim object 2,
        ...

        "dim_N": cd3.core.Dim object N
    }
    
    Parameters
    ----------
    obj : cds3.core.Object
        The CDSGroup or CDSVar python object
    
    Returns
    -------
    - Dictionary verison of the dimensions, empty dictionary if no
        dimensions
    """

    dims = obj.get_dims()
    dim_dict = {}

    for dim in dims:
        name = dim.get_name()
        dim_dict[name] = dim

    return dim_dict

def _get_att_dict(cds3.core.Object obj):
    """Make python dictionary of attributes

    The dictionary will be in the following form:

    {
        "att_1": cd3.core.Att object 1,
        "att_2": cd3.core.Att object 2,
        ...

        "att_N": cd3.core.Att object N
    }
    
    Parameters
    ----------
    obj : cds3.core.Object
        The CDSGroup or CDSVar python object
    
    Returns
    -------
    - Dictionary verison of the attributes, empty dictionary if no
        attributes
    """

    atts = obj.get_atts()

    att_dict = {}

    for att in atts:
        name = att.get_name()
        att_dict[name] = att

    return att_dict

def make_dataset_dict(cds3.core.Group root):
    """Make python dictionary of a group

    {
        "dimensions":
              dimension dictionary,
        "atts":
            attribute dictionary,
        "variables":
            variable dictionary,
        "groups":
            group dictionary,
        "ref":
            cd3.core.Group object for root,
        "parent":
            cd3.core.Group parent object for root (None if no parent)
        
    }
    
    Parameters
    ----------
    root : cds3.core.Group
        CDSGroup Python object

    Returns
    -------
    dictionary
        Dictionary version of the root group, None if error occurs

    """
    dataset = {}

    group_dict = _get_group_dict(root)
    if group_dict is None:
        return None

    dataset["groups"] = group_dict

    var_dict = _get_var_dict(root)
    if var_dict is None:
        return None

    dataset["variables"] = var_dict
    dataset["dimensions"] = _get_dim_dict(root)

    # Will come back to path
    # dataset["path"] = "/"

    dataset["parent"] = root.get_parent()
    dataset["ref"] = root

    dataset["atts"] = _get_att_dict(root)

    return dataset

##############################################################################
# ported from dsproc_hooks.c
##############################################################################
_user_data = None
_init_process_hook = None
_finish_process_hook = None
_process_data_hook = None
_pre_retrieval_hook = None
_post_retrieval_hook = None
_pre_transform_hook = None
_post_transform_hook = None
_process_file_hook = None
_quicklook_hook = None

def set_init_process_hook(hook):
    """Set hook function to call when a process is first initialized.
    
    This function must be called from the main function before dsproc_main()
    is called.
    
    The specified init_process_hook() function will be called once just before
    the main data processing loop begins and before the initial database
    connection is closed.
    
    The init_process_hook() function does not take any arguments, but it must
    return:

      - A user defined data structure or value (a Python object) that will be
        passed in as user_data to all other hook functions.
      - 1 if no user data is returned.
      - None if a fatal error occurred and the process should exit.
    
    Parameters
    ----------
    hook : function
        Function to call when the process is initialized.

    """
    global _init_process_hook
    _init_process_hook = hook

def set_finish_process_hook(hook):
    """Set hook function to call before a process finishes.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified finish_process_hook() function will be called once just after
    the main data processing loop finishes.  This function should be used to
    clean up any temporary files used, and to free any memory used by the
    structure returned by the init_process_hook() function.
    
    The finish_process_hook function must take the following argument:

      - object  user_data:  value returned by the init_process_hook() function
    
    Parameters
    ----------
    hook : function
        Function to call before the process finishes.

    """
    global _finish_process_hook
    _finish_process_hook = hook

def set_process_data_hook(hook):
    """Set the main data processing function.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified process_data_hook function will be called once per
    processing interval just after the output datasets are created, but
    before they are stored to disk.
    
    The process_data_hook function must take the following arguments:
    
      - object user_data: Value returned by the init_process_hook() function
      - time_t begin_date: The begin time of the current processing interval
      - time_t end_date:   The end time of the current processing interval
      - Group  input_data: The parent CDS.Group containing the input data.
    
    And must return:
    
      - 1 if processing should continue normally
      - 0 if processing should skip the current processing interval
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        The main data processing function.
    
    """
    global _process_data_hook
    _process_data_hook = hook

def set_pre_retrieval_hook(hook):
    """Set hook function to call before data is retrieved.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified pre_retrieval_hook function will be called once per
    processing interval just prior to data retrieval, and must take the
    following arguments:
    
    The pre_retrieval_hook function must take the following arguments:
    
      - object  user_data:  value returned by the init_process_hook() function
      - time_t  begin_date: the begin time of the current processing interval
      - time_t  end_date:   the end time of the current processing interval
    
    And must return:
    
      - 1 if processing should continue normally
      - 0 if processing should skip the current processing interval
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        Function to call before the data is retrieved.

    """
    global _pre_retrieval_hook
    _pre_retrieval_hook = hook

def set_post_retrieval_hook(hook):
    """Set hook function to call after data is retrieved.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified post_retrieval_hook function will be called once per
    processing interval just after data retrieval, but before the retrieved
    observations are merged and QC is applied.
    
    The post_retrieval_hook function must take the following arguments:
    
      - object user_data:  value returned by the init_process_hook() function
      - time_t begin_date: the begin time of the current processing interval
      - time_t end_date:   the end time of the current processing interval
      - Group  ret_data:   the parent CDS.Group containing the retrieved data
    
    And must return:
    
      - 1 if processing should continue normally
      - 0 if processing should skip the current processing interval
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        Function to call after data is retrieved.

    """
    global _post_retrieval_hook
    _post_retrieval_hook = hook

def set_pre_transform_hook(hook):
    """Set hook function to call before the data is transformed.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified pre_transform_hook function will be called once per
    processing interval just prior to data transformation, and after
    the retrieved observations are merged and QC is applied.
    
    The pre_transform_hook function must take the following arguments:
    
      - object user_data:  value returned by the init_process_hook() function
      - time_t begin_date: the begin time of the current processing interval
      - time_t end_date:   the end time of the current processing interval
      - Group  ret_data:   the parent CDS.Group containing the retrieved data
    
    And must return:
    
      - 1 if processing should continue normally
      - 0 if processing should skip the current processing interval
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        Function to call before the data is transformed.
    
    """
    global _pre_transform_hook
    _pre_transform_hook = hook

def set_post_transform_hook(hook):
    """Set hook function to call after the data is transformed.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified post_transform_hook function will be called once per
    processing interval just after data transformation, but before the
    output datasets are created.
    
    The post_transform_hook function must take the following arguments:
    
      - object user_data:  value returned by the init_process_hook() function
      - time_t begin_date: the begin time of the current processing interval
      - time_t end_date:   the end time of the current processing interval
      - Group  trans_data: the parent CDS.Group containing the transformed data
    
    And must return:
    
      - 1 if processing should continue normally
      - 0 if processing should skip the current processing interval
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        Function to call after the data is transformed.
    
    """
    global _post_transform_hook
    _post_transform_hook = hook

def set_quicklook_hook(hook):
    """VAP or Ingest: Set hook function to call after all data is stored.

    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.

    The specified quicklook_hook function will be called once per
    processing interval just after all data is stored.

    The quicklook_hook function must take the following arguments:

      - object user_data:  value returned by the init_process_hook() function
      - time_t begin_date: the begin time of the current processing interval
      - time_t end_date:   the end time of the current processing interval

    And must return:

      - 1 if processing should continue normally
      - 0 if processing should skip the current processing interval
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        Function to call after all data is stored.

    """
    global _quicklook_hook
    _quicklook_hook = hook

def set_process_file_hook(hook):
    """Set the main file processing function.
    
    This function must be called from the main function before dsproc_main()
    is called, or from the init_process_hook() function.
    
    The specified process_file_hook function will be called once for
    for every file found in the input direcotyr, and it must take the
    following arguments:
    
      - object user_data:  value returned by the init_process_hook() function
      - char   input_dir:  the full path to the input directory
      - char   file_name:  the name of the file to process
    
    And must return:
    
      - 1 if processing should continue normally
      - 0 if processing should skip the file
        and continue on to the next one.
      - -1 if a fatal error occurred and the process should exit.

    Parameters
    ----------
    hook : function
        The main file processing function.
    
    """
    global _process_file_hook
    _process_file_hook = hook

def _run_init_process_hook():
    """Private: Run the init_process_hook function.
    
    Returns
    -------
    int
        1 if successful, 0 if an error occurred.
    
    """
    global _user_data
    cdef const char *status_text
    if _init_process_hook:
        debug_lv1("\n----- ENTERING INIT PROCESS HOOK -------\n")
        _user_data = _init_process_hook()
        if not _user_data:
            status_text = dsproc_get_status()

            # Comparing against b'\0' since status_text is a byte string
            # b'\0' is byte string version of '\0'
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by init_process_hook function\n")
            return 0
        debug_lv1("----- EXITING INIT PROCESS HOOK --------\n\n")
    return 1

def _run_finish_process_hook():
    """Private: Run the finish_process_hook function."""
    if _finish_process_hook:
        debug_lv1("\n----- ENTERING FINISH PROCESS HOOK -----\n")
        _finish_process_hook(_user_data)
        debug_lv1("----- EXITING FINISH PROCESS HOOK ------\n\n")

def _run_process_data_hook(
        time_t begin_date,
        time_t end_date,
        object input_data):
    """Private: Run the process_data_hook function.
   
    Parameters
    ---------- 
    begin_date : time_t
        The begin time of the current processing interval
    end_date : time_t
        The end time of the current processing interval
    input_data : object
        The parent CDS.Group containing the input data
    
    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current processing interval
        and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.
    
    """
    cdef const char *status_text
    cdef int   status
    status = 1
    if _process_data_hook:
        debug_lv1("\n----- ENTERING PROCESS DATA HOOK -------\n")
        status = _process_data_hook(
                _user_data, begin_date, end_date, input_data)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by process_data_hook function\n")
        debug_lv1("----- EXITING PROCESS DATA HOOK --------\n\n")

    return status

def _run_pre_retrieval_hook(
        time_t   begin_date,
        time_t   end_date):
    """Private: Run the pre_retrieval_hook function.
    
    Parameters
    ----------
    begin_date : time_t
        The begin time of the current processing interval
    end_date : time_t
        The end time of the current processing interval
    
    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current processing interval
        and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.
    
    """
    cdef const char *status_text
    cdef int   status
    status = 1
    if _pre_retrieval_hook:
        debug_lv1("\n----- ENTERING PRE-RETRIEVAL HOOK ------\n")
        status = _pre_retrieval_hook(_user_data, begin_date, end_date)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by pre_retrieval_hook function\n")
        debug_lv1("----- EXITING PRE-RETRIEVAL HOOK -------\n")

    return status

def _run_post_retrieval_hook(
        time_t begin_date,
        time_t end_date,
        object ret_data):
    """Private: Run the post_retrieval_hook function.
    
    Parameters
    ----------
    begin_date : time_t
        The begin time of the current processing interval
    end_date : time_t
        The end time of the current processing interval
    ret_data : object
        The parent CDS.Group containing the retrieved data 
    
    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current processing interval
           and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.
    
    """
    cdef const char *status_text
    cdef int   status
    status = 1
    if _post_retrieval_hook:
        debug_lv1("\n----- ENTERING POST-RETRIEVAL HOOK -----\n")
        status = _post_retrieval_hook(
            _user_data, begin_date, end_date, ret_data)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by post_retrieval_hook function\n")
        debug_lv1("----- EXITING POST-RETRIEVAL HOOK ------\n\n")

    return status

def _run_pre_transform_hook(
        time_t begin_date,
        time_t end_date,
        object ret_data):
    """Private: Run the pre_transform_hook function.
    
    Parameters
    ----------
    begin_date : time_t
        The begin time of the current processing interval
    end_date : time_t
        The end time of the current processing interval
    ret_data : object
        The parent CDS.Group containing the retrieved data 
    
    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current processing interval
        and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.
    
    """
    cdef const char *status_text
    cdef int   status
    status = 1
    if _pre_transform_hook:
        debug_lv1("\n----- ENTERING PRE-TRANSFORM HOOK ------\n")
        status = _pre_transform_hook(
            _user_data, begin_date, end_date, ret_data)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by pre_transform_hook function\n")
        debug_lv1("----- EXITING PRE-TRANSFORM HOOK -------\n\n")

    return status

def _run_post_transform_hook(
        time_t begin_date,
        time_t end_date,
        object trans_data):
    """Private: Run the post_transform_hook function.
   
    Parameters
    ---------- 
    begin_date : time_t
        The begin time of the current processing interval
    end_date : time_t
        The end time of the current processing interval
    trans_data : object
        The parent CDS.Group containing the transformed data
    
    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current processing interval
        and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.
    
    """
    cdef const char *status_text
    cdef int   status
    status = 1
    if _post_transform_hook:
        debug_lv1("\n----- ENTERING POST-TRANSFORM HOOK -----\n")
        status = _post_transform_hook(
            _user_data, begin_date, end_date, trans_data)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by post_transform_hook function\n")
        debug_lv1("----- EXITING POST-TRANSFORM HOOK ------\n\n")

    return status

def _run_quicklook_hook(
        time_t begin_date,
        time_t end_date):
    """Private: Run the quicklook_hook function.
    
    Parameters
    ----------
    begin_date : time_t
        The begin time of the current processing interval
    end_date : time_t
        The end time of the current processing interval

    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current processing interval
        and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.

    """
    cdef const char *status_text
    cdef int   status
    status = 1
    if _quicklook_hook:
        debug_lv1("\n----- ENTERING QUICKLOOK HOOK -----\n")
        status = _quicklook_hook(
            _user_data, begin_date, end_date)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by quicklook_hook function\n")
        debug_lv1("----- EXITING QUICKLOOK HOOK ------\n\n")

    return status

def _run_process_file_hook(
        object input_dir,
        object file_name):
    """Private: Run the process_file_hook function.
   
    Parameters
    ---------- 
    input_dir : object
         Full path to the input directory
    file_name : object
         Name of the file to process
    
    Returns
    -------
    - 1 if processing should continue normally
    - 0 if processing should skip the current file
        and continue on to the next one.
    - -1 if a fatal error occurred and the process should exit.
    
    """
    cdef const char *status_text
    cdef int   status

    status = 1
    if _process_file_hook:
        debug_lv1("\n----- ENTERING PROCESS FILE HOOK -----\n")
        status = _process_file_hook(
            _user_data, input_dir, file_name)
        if status < 0:
            status_text = dsproc_get_status()
            if len(status_text) == 0 or status_text.startswith(b'\0'):
                error("Unknown Data Processing Error (check logs)","Error message not set by process_file_hook function\n")
        debug_lv1("----- EXITING PROCESS FILE HOOK ------\n\n")

    return status
##############################################################################
# ported from dsproc3.h
##############################################################################

# WHen used with the PyCapsule_Destructor, do I need to use 
# PyCapsule_GetPointer and free that instead?
#
# helper function for freeing memory allocated by libdsproc3
#cdef void _free(void *address):
#    free(address)

# I Think this one, since when destructor called, passes 
# capsule as its argument
cdef void _free(object obj):
    cdef void *address = NULL
    address = PyCapsule_GetPointer(obj, NULL)
    free(address)

# Potentially alter these cdef inline functions, need a better idea
# if the CDS_CHAR stuff needs to be altered....

# FOR CDS_CHAR, should I assume input will be byte string (i.e. b'e') or
# unicode (python3) stringi ('e')?
# initial value is python object, so assume python string???
# I believe char in this case is a character value (0-255)
cdef inline void* _alloc_single(CDSDataType cds_type, object initial_value=None):
    """Allocates a single value of the given type and return its address."""
    cdef void *retval = NULL
    if cds_type == CDS_NAT:
        raise ValueError("CDS_NAT")
    elif cds_type == CDS_CHAR:
        retval = malloc(sizeof(char))
        if initial_value is not None:
            (<char*>retval)[0] = initial_value
    elif cds_type == CDS_BYTE:
        retval = malloc(sizeof(char))
        if initial_value is not None:
            (<char*>retval)[0] = initial_value
    elif cds_type == CDS_SHORT:
        retval = malloc(sizeof(short))
        if initial_value is not None:
            (<short*>retval)[0] = initial_value
    elif cds_type == CDS_INT:
        retval = malloc(sizeof(int))
        if initial_value is not None:
            (<int*>retval)[0] = initial_value
    elif cds_type == CDS_INT64:
        retval = malloc(sizeof(long long))
        if initial_value is not None:
            (<long long*>retval)[0] = initial_value
    elif cds_type == CDS_FLOAT:
        retval = malloc(sizeof(float))
        if initial_value is not None:
            (<float*>retval)[0] = initial_value
    elif cds_type == CDS_DOUBLE:
        retval = malloc(sizeof(double))
        if initial_value is not None:
            (<double*>retval)[0] = initial_value
    else:
        raise ValueError( "Unknown CDSDataType %s" % cds_type)
    return retval

cdef inline object _convert_single(CDSDataType cds_type, void *value):
    """Converts a single value at the given address to the given type
    and frees it."""
    cdef object retval
    if cds_type == CDS_NAT:
        raise ValueError("CDS_NAT")
    elif cds_type == CDS_CHAR:
        retval = (<char*>value)[0];
        free(<char*>value)
    elif cds_type == CDS_BYTE:
        retval = (<char*>value)[0];
        free(<char*>value)
    elif cds_type == CDS_SHORT:
        retval = (<short*>value)[0];
        free(<short*>value)
    elif cds_type == CDS_INT:
        retval = (<int*>value)[0];
        free(<int*>value)
    elif cds_type == CDS_INT64:
        retval = (<long long*>value)[0];
        free(<long long*>value)
    elif cds_type == CDS_FLOAT:
        retval = (<float*>value)[0];
        free(<float*>value)
    elif cds_type == CDS_DOUBLE:
        retval = (<double*>value)[0];
        free(<double*>value)
    else:
        raise ValueError("Unknown CDSDataType %s" % cds_type )
    return retval

# Need to research how numpy interacts with Python3 strings
# Might need to alter here for CDS_CHAR
# According to numpy documentaion, NPY_STRING is for ASCII strings,
# so should still correspond with C strings. Just have to make sure
# that the data being set for the np array are byte strings from C
# Unsure if ever setting with Python Strings (unicode for python3)
cdef inline int cds_type_to_dtype(CDSDataType cds_type) except -1:
# Use np.NPY_<type> when calling into numpy C interface
# i.e. PyArray
    if cds_type == CDS_NAT:
        raise ValueError( "CDS_NAT")
    elif cds_type == CDS_CHAR:
        return np.NPY_STRING
    elif cds_type == CDS_BYTE:
        return np.NPY_BYTE
    elif cds_type == CDS_SHORT:
        return np.NPY_SHORT
    elif cds_type == CDS_INT:
        return np.NPY_INT
    elif cds_type == CDS_INT64:
        return np.NPY_INT64
    elif cds_type == CDS_FLOAT:
        return np.NPY_FLOAT
    elif cds_type == CDS_DOUBLE:
        return np.NPY_DOUBLE
    else:
        raise ValueError("Unknown CDSDataType %s" % cds_type)

cpdef inline np.dtype cds_type_to_dtype_obj(CDSDataType cds_type):
# Use np.dtype<type> when calling into numpy Python interface
    """Converts a CDSDataType to a dtype instance."""
    if cds_type == CDS_NAT:
        raise ValueError("CDS_NAT")
    elif cds_type == CDS_CHAR:
        return np.dtype(np.uint8)
    elif cds_type == CDS_BYTE:
        return np.dtype(np.int8)
    elif cds_type == CDS_SHORT:
        return np.dtype(np.int16)
    elif cds_type == CDS_INT:
        return np.dtype(np.int32)
    elif cds_type == CDS_INT64:
        return np.dtype(np.int64)
    elif cds_type == CDS_FLOAT:
        return np.dtype(np.float32)
    elif cds_type == CDS_DOUBLE:
        return np.dtype(np.float64)
    else:
        raise ValueError("Unknown CDSDataType %s" % cds_type)

# 'S1' is 1 character byte string, so should be okay, assuming 
#need to alter np.str though, that will reflect to unicode in Python 3 i believe,
#just want bytes, do not need np.str since that is unicode
#used in set_var_data
cpdef inline int dtype_to_cds_type(np.dtype dtype) except -1:
    if dtype == np.dtype(np.uint8):
        return CDS_CHAR
    elif dtype == np.dtype('S1'):
        return CDS_CHAR
    elif dtype == np.dtype(np.int8):
        return CDS_BYTE
    elif dtype == np.dtype(np.int16):
        return CDS_SHORT
    elif dtype == np.dtype(np.int32):
        return CDS_INT
    elif dtype == np.dtype(np.int64):
        return CDS_INT64
    elif dtype == np.dtype(np.float32):
        return CDS_FLOAT
    elif dtype == np.dtype(np.float64):
        return CDS_DOUBLE
    elif np.issubdtype(dtype, np.datetime64):
        return CDS_DOUBLE
    else:
        raise ValueError("Unknown dtype %s" % dtype)

# Convert bytes to unicode
def get_debug_level():
    """Get the current debug level."""
    return dsproc_get_debug_level()

def get_site():
    """Get the process site."""
    return _to_python_string( dsproc_get_site() )

def get_facility():
    """Get the process facility."""
    return _to_python_string( dsproc_get_facility() )

def get_name():
    """Get the process name."""
    return _to_python_string( dsproc_get_name() )

def get_datastream_id(
        object site,
        object facility,
        object dsc_name,
        object dsc_level,
        enums.DSRole role):
    """Get the ID of a datastream.
   
    Parameters
    ---------- 
    site : object
        Site name, or NULL to find first match
    facility : object
        Facility name, or NULL to find first match
    dsc_name : object
        Datastream class name
    dsc_level : object
        Datastream class level
    role : enums.DSRole
        Specifies input or output datastream
    
    Returns
    -------
    - Datastream ID
    - -1 if the datastream has not beed defined

    """
    
    cdef object b_site = _to_byte_c_string(site)
    cdef object b_facility = _to_byte_c_string(facility)
    cdef object b_dsc_name = _to_byte_c_string(dsc_name)
    cdef object b_dsc_level = _to_byte_c_string(dsc_level)

    return dsproc_get_datastream_id(b_site, b_facility, b_dsc_name, b_dsc_level, role)

def get_input_datastream_id(object dsc_name, object dsc_level):
    """Get the ID of an input datastream.
    
    This function will generate an error if the specified datastream class has
    not been defined in the database as an input for this process.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    dsc_name : object
        Datastream class name
    dsc_level : object
        Datastream class level
    
    Returns
    -------
    - Datastream ID
    - -1 if an error occurs

    """
    cdef object b_dsc_name = _to_byte_c_string(dsc_name)
    cdef object b_dsc_level = _to_byte_c_string(dsc_level)

    return dsproc_get_input_datastream_id(b_dsc_name, b_dsc_level)

def get_input_datastream_ids():
    """Get the IDs of all input datastreams.
    
    This function will return an array of all input datastream ids. The memory
    used by the returned array is dynamically allocated and will be freed when
    the returned ndarray goes out of scope.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Returns
    -------
    - Ndarray of datastream IDs
    - MemoryError if memory allocation error occurs

    """
    cdef int *ids
    cdef int count
    cdef np.npy_intp dims
    cdef np.ndarray[np.int32_t, ndim=1] ids_nd
    count = dsproc_get_input_datastream_ids(&ids)
    if count == -1:
        raise MemoryError
    if count == 0: # TODO: does this happen?
        return None
    dims = count
    ids_nd = np.PyArray_SimpleNewFromData(1, &dims, np.NPY_INT32, ids)
    # allow numpy to reclaim memory when array goes out of scope
    ids_nd.base = PyCapsule_New(ids,NULL, <PyCapsule_Destructor>_free)
    return ids_nd

def get_output_datastream_id(object dsc_name, object dsc_level):
    """Get the ID of an output datastream.
    
    This function will generate an error if the specified datastream class
    has not been defined in the database as an output for this process.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    dsc_name : object
        Datastream class name
    dsc_level : object
        Datastream class level
    
    Returns
    -------
    - Datastream ID
    - -1 if an error occurs

    """

    cdef object b_dsc_name = _to_byte_c_string(dsc_name)
    cdef object b_dsc_level = _to_byte_c_string(dsc_level)

    return dsproc_get_output_datastream_id(b_dsc_name, b_dsc_level)

def get_output_datastream_ids():
    """Get the IDs of all output datastreams.
    
    This function will return an array of all input datastream ids. The memory
    used by the returned array is dynamically allocated and will be freed when
    the returned ndarray goes out of scope.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
   
    Returns
    ------- 
    - Ndarray of datastream IDs

    Raise
    -----
    - MemoryError if memory allocation error occurs

    """
    cdef int *ids
    cdef int count
    cdef np.npy_intp dims
    cdef np.ndarray[np.int32_t, ndim=1] ids_nd
    count = dsproc_get_output_datastream_ids(&ids)
    if count == -1:
        raise MemoryError
    if count == 0: # TODO: does this happen?
        return None
    dims = count
    ids_nd = np.PyArray_SimpleNewFromData(1, &dims, np.NPY_INT32, ids)
    # allow numpy to reclaim memory when array goes out of scope
    ids_nd.base = PyCapsule_New(ids, NULL, <PyCapsule_Destructor>_free)
    return ids_nd

def datastream_name(int ds_id):
    """Returns the fully qualified datastream name.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    
    Returns
    -------
    - The datastream name
    - None if the datastream ID is not valid

    """
    cdef const char *retval = dsproc_datastream_name(ds_id)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def datastream_path(int ds_id):
    """Returns the path to the datastream directory.
    
    Parameters
    ----------
    ds_id : int 
        Datastream ID
    
    Returns
    -------
    - The path to the datastream directory
    - None if the datastream path has not ben set

    """
    cdef const char *retval = dsproc_datastream_path(ds_id)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def datastream_site(int ds_id):
    """Returns the datastream site.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    
    Returns
    -------
    - The datastream site code
    - None if the datastream ID is not valid

    """
    cdef const char *retval = dsproc_datastream_site(ds_id)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def datastream_facility(int ds_id):
    """ 995  * Returns the datastream facility.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    
    Returns
    -------
    - The datastream facility code
    - None if the datastream ID is not valid

    """
    cdef const char *retval = dsproc_datastream_facility(ds_id)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def datastream_class_name(int ds_id):
    """Returns the datastream class name.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    
    Returns
    -------
    - The datastream class name
    - None if the datastream ID is not valid

    """
    cdef const char *retval = dsproc_datastream_class_name(ds_id)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def datastream_class_level(int ds_id):
    """Returns the datastream class level.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    
    Returns
    -------
    - The datastream class level
    - None if the datastream ID is not valid

    """
    cdef const char *retval = dsproc_datastream_class_level(ds_id)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def dataset_name(cds3.core.Group dataset):
    """Returns the dataset name.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    
    Returns
    -------
    - Name of pointer to the dataset name
    - None if the specified dataset is None

    """
    cdef const char *retval
    if dataset is None:
        return None
    retval = dsproc_dataset_name(dataset.c_ob)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def getopt(object option):
    """Get a user defined command line option.
    
    Parameters
    ----------
    option : const char *
       The short or long option. 
    
    Returns
    -------
    - 1 if the option was specified on the command line
    - value if the option was specified on the command line and one was provided
    - None if the option was not specified on the command line

    """

    cdef object b_option
    cdef char *strval=NULL
    cdef int retval
    cdef object retstr

    b_option = _to_byte_c_string( option )

    retval = dsproc_getopt(b_option, &strval)
    if retval == 0:
        return None
    if strval != NULL:
        retstr = strval
        return _to_python_string(retstr)
    else:
        return retval

def setopt(
    object short_opt=None,
    object long_opt=None,
    object arg_name=None,
    object opt_desc=None):
    """ Set user defined command line option.
    This function must be called before calling dsproc_main.

    Parameters
    ----------
    short_opt : object
        Short options are single letters and are prefixed by a single 
        dash on the command line. Multiple short options can be grouped 
        behind a single dash. 
        Specify None for this argument if a short option should not be used.
    long_opt : object 
        Long options are prefixed by two consecutive dashes on the command line. 
        Specify None for this argument if a long option should not be used.
    arg_name : object 
        A single word description of the option argument to be used in the help message. 
        Specify None if this option does not take an argument on the command line. 
    opt_desc : object 
        A brief description of the option to be used in the help message.
    
    Returns
    -------
    - 1 if successful
    - 0 if the option has already been used or an error occurs.

    """
    cdef object b_short_opt
    cdef char c_short_opt
    cdef object b_long_opt
    cdef char *c_long_opt
    cdef object b_arg_name
    cdef char *c_arg_name
    cdef object b_opt_desc
    cdef char *c_opt_desc

    if short_opt is not None:
       b_short_opt = _to_byte_c_string(short_opt)
       c_short_opt = b_short_opt[0] #how to specify char of length 1
    if long_opt is not None:
       b_long_opt = _to_byte_c_string(long_opt)
       c_long_opt = b_long_opt
    else:
       c_long_opt = NULL
    if arg_name is not None:
       b_arg_name = _to_byte_c_string(arg_name)
       c_arg_name = b_arg_name
    else:
       c_arg_name = NULL

    b_opt_desc = _to_byte_c_string(opt_desc)

    if short_opt is not None:
        return dsproc_setopt(c_short_opt, c_long_opt, c_arg_name, b_opt_desc)
    else:
        return dsproc_setopt('\0', c_long_opt, c_arg_name, b_opt_desc)

def use_nc_extension():
    """ Set the default NetCDF file extension to 
    'nc' for output files. The NetCDF file extension used by
    ARM has historically been "cdf". This function can be used to
    change it to the new prefered extension of "nc", and must be
    called *before* calling dsproc_main().
    """
    dsproc_use_nc_extension()

def disable_lock_file():
    """ Disable the creation of the process lock file.

    Warning: Disabling the lock file will allow multiple
    processes to run over the top of themselves 
    and can lead to unpredictable behavior.
    """
    dsproc_disable_lock_file()

def get_output_dataset(int ds_id, int obs_index=0):
    """Get an output dataset.
    
    This function will return a cds3.Group of the output dataset for the
    specifed datastream and observation. The obs_index should always be zero
    unless observation based processing is being used. This is because all
    input observations should have been merged into a single observation
    in the output datasets.
    
    Parameters
    ----------
    ds_id : int
        Output datastream ID
    obs_index : int
        The index of the obervation to get the dataset for
    
    Returns
    -------
    - Dataset cds3.Group to the output dataset
    - NULL if it does not exist
    
    Example
    -------
    - Get the dataset for an output datastream
    - Code
        dsc_name = "example"
        dsc_level = "c1"
        ds_id = dsproc.get_output_datastream_id(dsc_name, dsc_level)
        dataset = dsproc.get_output_dataset(ds_id, 0) 

    """
    cdef cds3.core.Group group
    cdef CDSGroup *cds_group = NULL
    cds_group = dsproc_get_output_dataset(ds_id, obs_index)
    if cds_group == NULL:
        return None
    else:
        group = cds3.core.Group()
        group.set_group(cds_group)
        return group

def get_retrieved_dataset(int ds_id, int obs_index=0):
    """Get a retrieved dataset.
    
    This function will return a cds3.Group of the retrieved dataset for the
    specifed datastream and observation.
    
    The obs_index is used to specify which observation to get the dataset for.
    This value will typically be zero unless this function is called from a
    post_retrieval_hook() function, or the process is using observation based
    processing. In either of these cases the retrieved data will contain one
    "observation" for every file the data was read from on disk.
    
    It is also possible to have multiple observations in the retrieved data
    when a pre_transform_hook() is called if a dimensionality conflict
    prevented all observations from being merged.

    Parameters
    ----------
    ds_id : int
        Input datastream ID.
    obs_index : int, optional
        Observation index (Default value is 0, 0 based indexing).

    Returns
    -------
    cds3.core.Group
        Group of the retrieved dataset, or None if it does not exist.
    
    """
    cdef cds3.core.Group group
    cdef CDSGroup *cds_group = NULL
    cds_group = dsproc_get_retrieved_dataset(ds_id, obs_index)
    if cds_group == NULL:
        return None
    else:
        group = cds3.core.Group()
        group.set_group(cds_group)
        return group

def get_transformed_dataset(
        object coordsys_name,
        int ds_id,
        int obs_index=0):
    """Get a transformed dataset.
    
    This function will return a cds3.Group of the transformed dataset for the
    specifed coordinate system, datastream, and observation. The obs_index
    should always be zero unless observation based processing is being used.
    This is because all input observations should have been merged into a
    single observation in the transformed datasets.
    
    Parameters
    ----------
    coordsys_name : object
        The name of the coordinate system as specified in the
        retriever definition or None if a coordinate system name was not specified.
    
    ds_id : int
        Input datastream ID
    
    obs_index : int
        The index of the obervation to get the dataset for
    
    Returns
    -------
    - Cds3.Group of the output dataset
    - None if it does not exist

    """
    cdef object b_coordsys_name = _to_byte_c_string( coordsys_name )   

    cdef cds3.core.Group group
    cdef CDSGroup *cds_group = NULL
    cds_group = dsproc_get_transformed_dataset(b_coordsys_name, ds_id, obs_index)
    if cds_group == NULL:
        return None
    else:
        group = cds3.core.Group()
        group.set_group(cds_group)
        return group

def get_dim(cds3.core.Group dataset, object name):
    """Get a dimension from a dataset.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    name : object
        Name of the dimension
    
    Returns
    -------
    - Cds3.Dim of the dimension
    - None if the dimension does not exist

    """
    cdef cds3.core.Dim dim
    cdef CDSDim *cds_dim
    cdef object b_name = _to_byte_c_string(name)

    cds_dim = dsproc_get_dim(dataset.c_ob, b_name)
    if cds_dim == NULL:
        return None
    else:
        dim = cds3.core.Dim()
        dim.set_dim(cds_dim)
        return dim

def get_dim_length(cds3.core.Group dataset, object name):
    """Get the length of a dimension in a dataset.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    name : object
        Name of the dimension
    
    Returns
    -------
    - Dimension length
    - 0 if the dimension does not exist or has 0 length
    
    """
    cdef object b_name = _to_byte_c_string( name )

    return dsproc_get_dim_length(dataset.c_ob, b_name)

def set_dim_length(cds3.core.Group dataset, object name, size_t length):
    """Set the length of a dimension in a dataset.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    name : object
        Name of the dimension
    length : size_t
        New length of the dimension
    
    Returns
    -------
    - 1 if successful
    - 0 if the dimension does not exist
    - 0 if the dimension definition is locked
    - 0 if data has already been added to a variable using this dimension

    """
    cdef object b_name = _to_byte_c_string( name )

    return dsproc_set_dim_length(dataset.c_ob, b_name, length)

def change_att(
            cds3.core.Object parent,
            int          overwrite,
            object       name,
            CDSDataType  cds_type,
            object       value):
    """Change an attribute for a dataset or variable.
    
    This function will define the specified attribute if it does not exist.
    If the attribute does exist and the overwrite flag is set, the data type
    and value will be changed.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    parent : cds3.core.Object
        Pointer to the parent CDSGroup or CDSVar
    overwrite : int
        Overwrite flag (1 = TRUE, 0 = FALSE)
    name : object
        Attribute name
    cds_type : CDSDataType
        Attribute data type
    value : object
        Pointer to the attribute value
    
    Returns
    -------
    - 1 if successful
    - 0 if the parent object is not a group or variable
    - 0 if the parent group or variable definition is locked
    - 0 if the attribute definition is locked
    - 0 if a memory allocation error occurred

    """
    # TODO need to used fused types for the 'parent' param and also likely for
    # the conversion of the 'value' param
    cdef np.ndarray value_nd = np.asarray(value, cds_type_to_dtype_obj(cds_type))
    if value_nd.ndim == 0:
        value_nd = value_nd[None] # add dummy dimension to scalar value
    assert value_nd.ndim == 1

    cdef object b_name = _to_byte_c_string( name )

    return dsproc_change_att(parent.cds_object, overwrite, b_name, cds_type,
            len(value_nd), value_nd.data)

def get_att(cds3.core.Object parent, object name):
    """Get an attribute from a dataset or variable.
    
    Parameters
    ----------
    parent : cds3.core.Object
        The parent cds3.Group or cds3.Var
    name : object
        Name of the attribute
    
    Returns
    -------
    - Pointer to the attribute
    - None if the attribute does not exist
    
    """
    cdef cds3.core.Att att
    cdef CDSAtt *cds_att

    cdef object b_name = _to_byte_c_string( name )

    cds_att = dsproc_get_att(parent.cds_object, b_name)
    if cds_att == NULL:
        return None
    else:
        att = cds3.core.Att()
        att.set_att(cds_att)
        return att

def get_att_text(cds3.core.Object parent, object name):
    """Get a copy of an attribute value from a dataset or variable.
    
    This function will get a copy of an attribute value converted to a
    text string. If the data type of the attribute is not CDS_CHAR the
    cds_array_to_string() function is used to create the output string.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    parent : cds3.core.Object
        Pointer to the parent CDSGroup or CDSVar
    name : object
        Name of the attribute
    
    Returns
    -------
    - The output string
    - None if the attribute does not exist or has zero length
    - None if a memory allocation error occurs

    """
    cdef size_t length
    cdef char *text
    cdef object retval

    cdef object b_name = _to_byte_c_string( name )

    text = dsproc_get_att_text(parent.cds_object, b_name, &length, NULL)
    if text == NULL:
        return None
    retval = text
    free(text);
    return _to_python_string(retval)

def get_att_value(cds3.core.Object parent, object name, CDSDataType cds_type):
    """Get a copy of an attribute value from a dataset or variable.
    
    This function will get a copy of an attribute value casted into
    the specified data type. The functions cds_string_to_array() and
    cds_array_to_string() are used to convert between text (CDS_CHAR)
    and numeric data types.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    parent : cds3.core.Object
        Pointer to the parent CDSGroup or CDSVar
    name : object
        Name of the attribute
    cds_type : CDSDataType
        Data type of the output array
    
    Returns
    -------
    - Ndarray of the output array
    - None if the attribute value has zero length (length == 0)
    - None if a memory allocation error occurs (length == (size_t)-1)

    """
    cdef size_t length
    cdef void *value = NULL
    cdef int dtype
    cdef np.ndarray value_nd
    cdef np.npy_intp dims

    cdef object b_name = _to_byte_c_string( name )

    if cds_type == CDS_CHAR:
        return get_att_text(parent, name)

    value = dsproc_get_att_value(parent.cds_object, b_name, cds_type, &length, NULL)
    if value == NULL:
        return None

    dtype = cds_type_to_dtype(cds_type)
    dims = length
    value_nd = np.PyArray_SimpleNewFromData(1, &dims, dtype, value)

    # allow numpy to reclaim memory when array goes out of scope
    value_nd.base = PyCapsule_New(value, NULL, <PyCapsule_Destructor>_free)
    return value_nd

def set_att(cds3.core.Object parent, int overwrite, object name,
        CDSDataType cds_type, object value):
    """Set the value of an attribute in a dataset or variable.
    
    This function will define the specified attribute if it does not exist.
    If the attribute does exist and the overwrite flag is set, the value will
    be set by casting the specified value into the data type of the attribute.
    The functions cds_string_to_array() and cds_array_to_string() are used to
    convert between text (CDS_CHAR) and numeric data types.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    parent : cds3.core.Object
        Pointer to the parent CDSGroup or CDSVar
    overwrite : int
        Overwrite flag (1 = TRUE, 0 = FALSE)
    name : object
        Attribute name
    cds_type : CDSDataType
        Attribute data type
    value : object
        Pointer to the attribute value
    
    Returns
    -------
    - 1 if successful
    - 0 if the parent object is not a group or variable
    - 0 if the parent group or variable definition is locked
    - 0 if the attribute definition is locked
    - 0 if a memory allocation error occurred

    """
    cdef int return_value
    cdef np.ndarray value_nd 
    cdef object byte_value
    cdef object byte_name = _to_byte_c_string( name )
    
    if cds_type == CDS_CHAR:        
        byte_value = _to_byte_c_string(value)
        length = len(byte_value)
        value_nd = np.asarray(byte_value)
        
    else:
        value_nd = np.asarray(value, cds_type_to_dtype_obj(cds_type))

        if value_nd.ndim == 0:
            value_nd = value_nd[None] # add dummy dimension to a scalar value

        assert value_nd.ndim == 1
        
        length = len(value_nd)

    return_value = dsproc_set_att(parent.cds_object, overwrite, byte_name, cds_type,
        length, value_nd.data)

    return return_value

def set_att_text(cds3.core.Object parent, object name, object value):
    """Set the value of an attribute in a dataset or variable.
    
    The cds_string_to_array() function will be used to set the attribute
    value if the data type of the attribute is not CDS_CHAR.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
   
    Parameters
    ---------- 
    parent : cds3.core.Object
        Pointer to the parent CDSGroup or CDSVar
    name : object
        Name of the attribute
    value : object
        The value as a string
    
    Returns
    -------
    - 1 if successful
    - 0 if the attribute does not exist
    - 0 if the attribute definition is locked
    - 0 if a memory allocation error occurred

    """
    cdef object b_name = _to_byte_c_string( name )
    cdef object b_value = _to_byte_c_string( value )

    return dsproc_set_att_text(parent.cds_object, b_name, b_value)

def set_att_value(cds3.core.Object parent, object name,
        CDSDataType cds_type, object value):
    """Set the value of an attribute in a dataset or variable.
    
    This function will set the value of an attribute by casting the
    specified value into the data type of the attribute. The functions
    cds_string_to_array() and cds_array_to_string() are used to convert
    between text (CDS_CHAR) and numeric data types.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    parent : cds3.core.Object
        The parent CDSGroup or CDSVar
    name : object
        Name of the attribute
    cds_type : CDSDataType
        Data type of the specified value
    value : object
        The attribute value
    
    Returns
    -------
    - 1 if successful
    - 0 if the attribute does not exist
    - 0 if the attribute definition is locked
    - 0 if a memory allocation error occurred

    """
    cdef np.ndarray value_nd

    if cds_type == CDS_CHAR:
        return set_att_text(parent, name, value)
    
    value_nd = np.asarray(value, cds_type_to_dtype_obj(cds_type))

    if value_nd.ndim == 0:
        value_nd = value_nd[None] # add dummy dimension to a scalar value
    assert value_nd.ndim == 1
    length = len(value_nd)

    cdef object b_name = _to_byte_c_string( name )

    return dsproc_set_att_value(parent.cds_object, b_name, cds_type,
            length, value_nd.data)

def clone_var(
            cds3.core.Var src_var,
            cds3.core.Group dataset=None,
            object var_name="",
            CDSDataType data_type=CDS_NAT,
            object dim_names=None, # should be iterable i.e. 1-d list/tuple
            bint copy_data=True):
    """Create a clone of an existing variable.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
   
    Parameters
    ---------- 
    src_var : cds3.core.Var
        The source variable to clone
    dataset : cds3.core.Group
        Dataset to create the new variable in, or None to create
            the variable in the same dataset the source variable belongs to.
    var_name : object
        Name to use for the new variable, or None to use the
            source variable name.
    data_type : CDSDataType
        Data type to use for the new variable, or CDS_NAT to use
            the same data type as the source variable.
    dim_names : object
        The list of corresponding dimension names in the dataset
            the new variable will be created in, or None if the dimension names
            are the same.
    copy_data : bint
        Flag indicating if the data should be copied,
            (1 == TRUE, 0 == FALSE)
    
    Returns
    -------
    - The new variable
    - None if the variable already exists in the dataset
    - None if a memory allocation error occurred

    """
    cdef CDSVar *cds_var = NULL
    cdef cds3.core.Var var
    cdef const char **c_dim_names = NULL
    cdef CDSGroup *cds_group = NULL

    cdef object b_var_name = _to_byte_c_string( var_name )

    cdef object b_dim_names = None

    if data_type == CDS_NAT:
        data_type = src_var.c_ob.type
    if dataset is not None:
        cds_group = dataset.c_ob
    if dim_names is not None and len(dim_names) > 0:
        c_dim_names = <const_char**>malloc(len(dim_names) * sizeof(char*))

        #Make it a one D list
        b_dim_names = [None] * len(dim_names)

        # Convert dimension names to byte strings
        # String processing varies depending on which Python version is running
        # The PyString_AsString is different with different functionality depending on the Python version. See imports.
        if sys.version_info[0] < 3:
            # Python Major Version 2
            for i in range(len(dim_names)):
                c_dim_names[i] = PyString_AsString(dim_names[i])
        else:
            # Python Major Version 3
            for i in range(len(dim_names)):
                b_dim_names[i] = PyUnicode_AsEncodedString(dim_names[i],
                                    "UTF-8","strict")

            for i in range(len(b_dim_names)):
                c_dim_names[i] = PyString_AsString(b_dim_names[i])

    # cython doesn't like assigning NULL to char* variables!
    # but dsproc3 expects var_name to be NULL (we signal via empty string)
    if len(var_name) > 0:
        cds_var = dsproc_clone_var(src_var.c_ob, cds_group, b_var_name, data_type,
                c_dim_names, copy_data)
    else:
        cds_var = dsproc_clone_var(src_var.c_ob, cds_group, NULL, data_type,
                c_dim_names, copy_data)
    if dim_names is not None:
        free(c_dim_names)
        del b_dim_names
    if cds_var == NULL:
        return None
    else:
        var = cds3.core.Var()
        var.set_var(cds_var)
        return var

# I think min_char and max_char assignemnts should be fine for Python3 (not string
# declarations), but will keep an eye on just in case
def define_var(
            cds3.core.Group dataset,
            object name, #string
            CDSDataType cds_type,
            #int ndims, # derived from dim_names
            object dim_names, # should be iterable i.e. 1-d list/tuple
            object long_name="", #string
            object standard_name="", #string
            object units="", #string
            object valid_min=None,
            object valid_max=None,
            object missing_value=None,
            object fill_value=None):
    """Define a new variable in an existing dataset.
    
    This function will define a new variable with all standard attributes.
    Any of the attribute values can be NULL to indicate that the attribute
    should not be created.
    
    Description of Attributes:
    
    **long_name:**
    This is a one line description of the variable and should be suitable
    to use as a plot title for the variable.
    
    **standard_name:**
    This is defined in the CF Convention and describes the physical
    quantities being represented by the variable. Please refer to the
    "CF Standard Names" section of the CF Convention for the table of
    standard names.
    
    **units:**
    This is the units string to use for the variable and must be
    recognized by the UDUNITS-2 libary.
    
    **valid_min:**
    The smallest value that should be considered to be a valid data value.
    The specified value must be the same data type as the variable.
    
    **valid_max:**
    The largest value that should be considered to be a valid data value.
    The specified value must be the same data type as the variable.
    
    **missing_value:**
    This comes from an older NetCDF convention and has been used by ARM
    for almost 2 decades. The specified value must be the same data type
    as the variable.
    
    **_FillValue:**
    Most newer conventions specify the use of _FillValue over missing_value.
    The value of this attribute is also recognized by the NetCDF library and
    will be used to initialize the data values on disk when the variable is
    created. Tools like ncdump will also display fill values as _ so they
    can be easily identified in a text dump. The libdsproc3 library allows
    you to use both missing_value and _FillValue and they do not need to be
    the same. The specified value must be the same data type as the variable.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    name : object
        Name of the variable
    cds_type : CDSDataType
        Data type of the variable
    dim_names : object
        Iterable i.e. 1-d list/tuple of dimension names
    long_name : object
        String to use for the long_name attribute
    standard_name : object
        String to use for the standard_name attribute
    units : object
        String to use for the units attribute
    valid_min : object
        Void pointer to the valid_min
    valid_max : object
        Void pointer to the valid_max
    missing_value : object
        Void pointer to the missing_value
    fill_value : object
        Void pointer to the _FillValue
    
    Returns
    -------
    - Pointer to the new variable
    - NULL if an error occurred
    """
    cdef CDSVar *cds_var = NULL
    cdef cds3.core.Var var
    cdef np.dtype dtype
    cdef void *min_ptr=NULL
    cdef void *max_ptr=NULL
    cdef void *missing_ptr=NULL
    cdef void *fill_ptr=NULL
    cdef char min_char, max_char, missing_char, fill_char
    cdef signed char min_byte, max_byte, missing_byte, fill_byte
    cdef short min_short, max_short, missing_short, fill_short
    cdef int min_int, max_int, missing_int, fill_int
    cdef long long min_long, max_long, missing_long, fill_long
    cdef float min_float, max_float, missing_float, fill_float
    cdef double min_double, max_double, missing_double, fill_double
    cdef const char **c_dim_names = <const_char**>malloc(len(dim_names) * sizeof(char*))
    cdef char *c_long_name
    cdef char *c_standard_name
    cdef char *c_units
   
    # For unicode to bytes conversion 
    cdef object b_name = _to_byte_c_string(name)
    cdef object b_long_name = None
    cdef object b_standard_name = None
    cdef object b_units = None
    cdef object b_dim_names = [None] * len(dim_names)
    
    if len(long_name) != 0:
        b_long_name = _to_byte_c_string( long_name )
        c_long_name = b_long_name
    else:
        c_long_name = NULL
    if len(standard_name) != 0:
        b_standard_name = _to_byte_c_string( standard_name )
        c_standard_name = b_standard_name
    else:
        c_standard_name = NULL
    if len(units) != 0:
        b_units = _to_byte_c_string( units )
        c_units = b_units
    else:
        c_units = NULL

    
    # Convert dimension names to byte strings
    # String processing varies depending on which Python version is running
    # The PyString_AsString is different with different functionality depending on the Python version. See imports.
    if sys.version_info[0] < 3:
        # Python Major Version 2
        for i in range(len(dim_names)):
            c_dim_names[i] = PyString_AsString(dim_names[i])
    else:
        # Python Major Version 3
        for i in range(len(dim_names)):
            b_dim_names[i] =  PyUnicode_AsEncodedString(dim_names[i],
                                "UTF-8","strict")
        for i in range(len(b_dim_names)):
            c_dim_names[i] = PyString_AsString(b_dim_names[i])

    if cds_type == CDS_NAT:
        raise ValueError("CDS_NAT")
    elif cds_type == CDS_CHAR:
        if valid_min is not None:
            min_char = valid_min
            min_ptr = &min_char
        if valid_max is not None:
            max_char = valid_max
            max_ptr = &max_char
        if missing_value is not None:
            missing_char = missing_value
            missing_ptr = &missing_char
        if fill_value is not None:
            fill_char = fill_value
            fill_ptr = &fill_char
    elif cds_type == CDS_BYTE:
        if valid_min is not None:
            min_byte = valid_min
            min_ptr = &min_byte
        if valid_max is not None:
            max_byte = valid_max
            max_ptr = &max_byte
        if missing_value is not None:
            missing_byte = missing_value
            missing_ptr = &missing_byte
        if fill_value is not None:
            fill_byte = fill_value
            fill_ptr = &fill_byte
    elif cds_type == CDS_SHORT:
        if valid_min is not None:
            min_short = valid_min
            min_ptr = &min_short
        if valid_max is not None:
            max_short = valid_max
            max_ptr = &max_short
        if missing_value is not None:
            missing_short = missing_value
            missing_ptr = &missing_short
        if fill_value is not None:
            fill_short = fill_value
            fill_ptr = &fill_short
    elif cds_type == CDS_INT:
        if valid_min is not None:
            min_int = valid_min
            min_ptr = &min_int
        if valid_max is not None:
            max_int = valid_max
            max_ptr = &max_int
        if missing_value is not None:
            missing_int = missing_value
            missing_ptr = &missing_int
        if fill_value is not None:
            fill_int = fill_value
            fill_ptr = &fill_int
    elif cds_type == CDS_INT64:
        if valid_min is not None:
            min_long = valid_min
            min_ptr = &min_long
        if valid_max is not None:
            max_long = valid_max
            max_ptr = &max_long
        if missing_value is not None:
            missing_long = missing_value
            missing_ptr = &missing_long
        if fill_value is not None:
            fill_long = fill_value
            fill_ptr = &fill_long
    elif cds_type == CDS_FLOAT:
        if valid_min is not None:
            min_float = valid_min
            min_ptr = &min_float
        if valid_max is not None:
            max_float = valid_max
            max_ptr = &max_float
        if missing_value is not None:
            missing_float = missing_value
            missing_ptr = &missing_float
        if fill_value is not None:
            fill_float = fill_value
            fill_ptr = &fill_float
    elif cds_type == CDS_DOUBLE:
        if valid_min is not None:
            min_double = valid_min
            min_ptr = &min_double
        if valid_max is not None:
            max_double = valid_max
            max_ptr = &max_double
        if missing_value is not None:
            missing_double = missing_value
            missing_ptr = &missing_double
        if fill_value is not None:
            fill_double = fill_value
            fill_ptr = &fill_double
    else:
        raise ValueError("invalid CDSType")

    cds_var = dsproc_define_var(dataset.c_ob, b_name, cds_type, len(dim_names),
            c_dim_names, c_long_name, c_standard_name, c_units,
            min_ptr, max_ptr, missing_ptr, fill_ptr)

    if dim_names is not None:
        free(c_dim_names)
        del b_dim_names
    if cds_var == NULL:
        return None
    else:
        var = cds3.core.Var()
        var.set_var(cds_var)
        return var


def delete_var(cds3.core.Var var):
    """Delete a variable from a dataset.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    
    Returns
    -------
    - 1 if the variable was deleted (or the input var was NULL)
    - 0 if the variable is locked
    - 0 if the group is locked

    """
    cdef int return_value = dsproc_delete_var(var.c_ob)
    if return_value == 1:
        var.set_var(NULL)
    return return_value

def get_coord_var(cds3.core.Var _var, int dim_index):
    """Get the coordinate variable for a variable's dimension.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    dim_index : int
        Index of the dimension
    
    Returns
    -------
    - Pointer to the coordinate variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cds_var = dsproc_get_coord_var(_var.c_ob, dim_index)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_dataset_vars(
            cds3.core.Group dataset,
            object var_names,
            bint required,
            bint qc_vars=False,
            bint aqc_vars=False):
    """Get variables and companion QC variables from a dataset.
    
    If nvars is 0 or var_names is None, the output vars array will contain
    the pointers to the variables that are not companion QC variables. In
    this case the variables in the vars array will be in the same order they
    appear in the dataset. The following time and location variables will be
    excluded from this array:
    
    - base_time
    - time_offset
    - time
    - lat
    - lon
    - alt

    If nvars and var_names are specified, the output vars array will contain
    an entry for every variable in the var_names list, and will be in the
    specified order. Variables that are not found in the dataset will have
    a NULL value if the required flag is set to 0. If the required flag is
    set to 1 and a variable does not exist, an error will be generated.
    
    If the qc_vars argument is not NULL it will contain the pointers to the
    companion qc\_ variables. Likewise, if the aqc_vars argument is not NULL
    it will contain the pointers to the companion aqc\_ variables. If a
    companion QC variable does not exist for a variable, the corresponding
    entry in the QC array will be NULL.
    
    The memory used by the returned arrays belongs to a 'dsproc_user\_'
    CDSVarGroup defined in the dataset and must *NOT* be freed by the calling
    process.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.

    Parameters
    ----------
    dataset : cds3.core.Goup
        The dataset to retrieve variables for.
    var_names : list of strings
        List of variable names.
    required : bool
        Specifies if all variables in the names list are required.
    qc_vars : bool, optional
        Whether to also return qc_vars (Default is False, qc_vars not returned).
    aqc_vars : bool, optional
        Whether to also return aqc_vars (Default is False, aqc_vars not returned).


    Returns
    -------
    - Variables if qc_vars==False and aqc_vars==False
    - (Variables,qc_vars) if qc_vars==True and aqc_vars==False
    - (Variables,aqc_vars) if qc_vars==False and aqc_vars==True
    - (Variables,qc_vars,aqc_vars) if qc_vars==True and aqc_vars==True
    - If error, None if qc_vars==False and aqc_vars==False
    - If error, (None,None) if qc_vars==True and aqc_vars==False
    - If error, (None,None) if qc_vars==False and aqc_vars==True
    - If error, (None,None,None) if qc_vars==True and aqc_vars==True

    """
    cdef int length
    cdef cds3.core.Var var_tmp
    cdef CDSVar **_vars = NULL
    cdef CDSVar **_qc_vars = NULL
    cdef CDSVar **_aqc_vars = NULL
    cdef const char **c_var_names = NULL

    #unicode to bytes conversion
    cdef object b_var_names = None

    if var_names:
        c_var_names = <const_char**>malloc((len(var_names)+1) * sizeof(char*))

        b_var_names = [None] * (len(var_names)+1)

        #Convert var names to bytes strings
        # String processing varies depending on which Python version is running
        # The PyString_AsString is different with different functionality depending on the Python version. See imports.
        if sys.version_info[0] < 3:
            # Python Major Version 2
            for i in range(len(var_names)):
                c_var_names[i] = PyString_AsString(var_names[i])
        else:
            # Python Major Version 3
            for i in range(len(var_names)):
                b_var_names[i] = PyUnicode_AsEncodedString(var_names[i], "UTF-8","strict")

            for i in range(len(var_names)):
                c_var_names[i] = PyString_AsString(b_var_names[i])

        c_var_names[len(var_names)] = NULL
    if qc_vars and aqc_vars:
        length = dsproc_get_dataset_vars(dataset.c_ob, c_var_names, required,
                &_vars, &_qc_vars, &_aqc_vars)
    elif qc_vars:
        length = dsproc_get_dataset_vars(dataset.c_ob, c_var_names, required,
                &_vars, &_qc_vars, NULL)
    elif aqc_vars:
        length = dsproc_get_dataset_vars(dataset.c_ob, c_var_names, required,
                &_vars, NULL, &_aqc_vars)
    else:
        length = dsproc_get_dataset_vars(dataset.c_ob, c_var_names, required,
                &_vars, NULL, NULL)
    if var_names:
        free(c_var_names)
        del b_var_names
    if -1 == length:
        if qc_vars and aqc_vars:
            return None,None,None
        elif qc_vars or aqc_vars:
            return None,None
        else:
            return None
    ret_vars = [None]*length
    ret_qc_vars = [None]*length
    ret_aqc_vars = [None]*length
    for i in range(length):
        if _vars and _vars[i]:
            var_tmp = cds3.core.Var()
            var_tmp.set_var(_vars[i])
            ret_vars[i] = var_tmp
        if qc_vars and _qc_vars and _qc_vars[i]:
            var_tmp = cds3.core.Var()
            var_tmp.set_var(_qc_vars[i])
            ret_qc_vars[i] = var_tmp
        if aqc_vars and _aqc_vars and _aqc_vars[i]:
            var_tmp = cds3.core.Var()
            var_tmp.set_var(_aqc_vars[i])
            ret_aqc_vars[i] = var_tmp
    if qc_vars and aqc_vars:
        return ret_vars,ret_qc_vars,ret_aqc_vars
    elif qc_vars:
        return ret_vars,ret_qc_vars
    elif aqc_vars:
        return ret_vars,ret_aqc_vars
    else:
        return ret_vars

def get_metric_var(cds3.core.Var var, object metric):
    """Get a companion metric variable for a variable.
    
    Known metrics at the time of this writing (so there may be others):
    
    - "frac": the fraction of available input values used
    - "std": the standard deviation of the calculated value
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    metric : object
        Name of the metric
    
    Returns
    -------
    - The metric variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var metric_var
    cdef object b_metric = _to_byte_c_string( metric )

    cds_var = dsproc_get_metric_var(var.c_ob, b_metric)
    if cds_var == NULL:
        return None
    metric_var = cds3.core.Var()
    metric_var.set_var(cds_var)
    return metric_var

def get_output_var(int ds_id, object var_name, int obs_index=0):
    """Get a variable from an output dataset.
    
    The obs_index should always be zero unless observation based processing is
    being used. This is because all input observations should have been merged
    into a single observation in the output datasets.
    
    Parameters
    ----------
    ds_id : int
        Output datastream ID
    var_name : object
        Variable name
    obs_index : int
        The index of the obervation to get the dataset for
    
    Returns
    -------
    - Pointer to the output variable
    - None if it does not exist

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cdef object b_var_name = _to_byte_c_string( var_name)

    cds_var = dsproc_get_output_var(ds_id, b_var_name, obs_index)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_qc_var(cds3.core.Var _var):
    """Get the companion QC variable for a variable.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    
    Returns
    -------
    - Pointer to the QC variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cds_var = dsproc_get_qc_var(_var.c_ob)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_retrieved_var(object var_name, int obs_index=0):
    """Get a primary variable from the retrieved data.
    
    This function will find a variable in the retrieved data that was
    explicitly requested by the user in the retriever definition.
    
    The obs_index is used to specify which observation to pull the variable
    from. This value will typically be zero unless this function is called
    from a post_retrieval_hook() function, or the process is using observation
    based processing. In either of these cases the retrieved data will
    contain one "observation" for every file the data was read from on disk.
    
    It is also possible to have multiple observations in the retrieved data
    when a pre_transform_hook() is called if a dimensionality conflict
    prevented all observations from being merged.
    
    Parameters
    ----------
    var_name : object
        Variable name
    obs_index : int
        The index of the obervation to get the variable from
    
    Returns
    -------
    - Pointer to the retrieved variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cdef object b_var_name = _to_byte_c_string( var_name) 

    cds_var = dsproc_get_retrieved_var(b_var_name, obs_index)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_transformed_var(object var_name, int obs_index=0):
    """Get a primary variable from the transformed data.
    
    This function will find a variable in the transformed data that was
    explicitly requested by the user in the retriever definition.
    
    The obs_index is used to specify which observation to pull the variable
    from. This value will typically be zero unless the process is using
    observation based processing. If this is the case the transformed data will
    contain one "observation" for every file the data was read from on disk.
    
    Parameters
    ----------
    var_name : object
        Variable name
    obs_index : int
        The index of the obervation to get the variable from
    
    Returns
    -------
    - Pointer to the transformed variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cdef object b_var_name = _to_byte_c_string( var_name)

    cds_var = dsproc_get_transformed_var(b_var_name, obs_index)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_trans_coordsys_var(object coordsys_name, object var_name, int obs_index=0):
    """Get a variable from a transformation coordinate system.
    
    Unlike the dsproc_get_transformed_var() function, this function will find
    any variable in the specified transformation coordinate system.
    
    The obs_index is used to specify which observation to pull the variable
    from. This value will typically be zero unless the process is using
    observation based processing. If this is the case the transformed data will
    contain one "observation" for every file the data was read from on disk.
    
    Parameters
    ----------
    coordsys_name : object
        Coordinate system name
    var_name :object
        Variable name
    obs_index : int
        The index of the obervation to get the variable from
    
    Returns
    -------
    - The transformed variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cdef object b_coordsys_name = _to_byte_c_string( coordsys_name )
    cdef object b_var_name = _to_byte_c_string( var_name )

    cds_var = dsproc_get_trans_coordsys_var(b_coordsys_name, b_var_name, obs_index)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_var(cds3.core.Group dataset, object name):
    """Get a variable from a dataset.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    name : object
        Name of the variable
    
    Returns
    -------
    - The variable
    - None if the variable does not exist

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cdef object b_name = _to_byte_c_string( name )

    cds_var = dsproc_get_var(dataset.c_ob, b_name)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def var_name(cds3.core.Var var):
    """Returns the variable name.
    
    Parameters
    ----------
    var : cds3.core.Var
        The variable
    
    Returns
    -------
    - The variable name
    - None if the specified variable is None

    """
    cdef const char *retval
    if var is None:
        return None
    retval = dsproc_var_name(var.c_ob)
    if retval == NULL:
        return None
    return _to_python_string( retval )

def var_sample_count(cds3.core.Var var):
    """Returns the number of samples in a variable's data array.
    
    The sample_count for a variable is the number of samples stored
    along the variable's first dimension.
    
    Parameters
    ----------
    var : cds3.core.Var
        The variable
    
    Returns
    -------
    - Number of samples in the variable's data array

    """
    return dsproc_var_sample_count(var.c_ob)

def var_sample_size(cds3.core.Var var):
    """Returns the sample size of a variable.
    
    Variables with less than 2 dimensions will always have a sample_size of 1.
    The sample_size for variables with 2 or more dimensions is the product of
    all the dimension lengths starting with the 2nd dimension.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    
    Returns
    -------
    - Sample size of the variable

    """
    return dsproc_var_sample_size(var.c_ob)

def alloc_var_data(cds3.core.Var var, size_t sample_start, size_t sample_count):
    """Allocate memory for a variable's data array.
    
    This function will allocate memory as necessary to ensure that the
    variable's data array is large enough to store another sample_count
    samples starting from sample_start.
    
    The data type of the returned array will be the same as the variable???s
    data type.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    var : cds3.core.Var
        The variable to allocate memory for
    sample_start : size_t
        Start sample (0 based indexing)
    sample_count : size_t
        Number of new samples to allocate memory for
    
    Returns
    -------
    - Data array starting at the specified start sample
    - None if one of the variable's static dimensions has 0 length
    - None if the variable has no dimensions, and sample_start is not
        equal to 0 or sample_count is not equal to 1.
    - None if the first variable dimension is not unlimited, and
        sample_start + sample_count would exceed the dimension length.
    - None if a memory allocation error occurred
    
    """
    cdef void *data = NULL
    data = dsproc_alloc_var_data(var.c_ob, sample_start, sample_count)
    if data == NULL:
        return None
    return var.get_datap(sample_start)

def alloc_var_data_index(cds3.core.Var var, size_t sample_start,
        size_t sample_count):
    """Allocate memory for a variable's data array.
    
    This function is the same as dsproc.alloc_var_data() except that
    it returns a data index starting at the specified start sample
    (see dsproc.get_var_data_index()). For variables that have less than
    two dimensions this function is identical to dsproc_alloc_var_data().
    It is up to the calling process to cast the return value of this
    function into the proper data type.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
   
    Parameters
    ---------- 
    var : cds3.core.Var
        Pointer to the specific variable
    sample_start : size_t
        Start sample (0 based indexing)
    sample_count : size_t
        Number of new samples to allocate memory for
    
    Returns
    -------
    - The data index into the variable's data array starting at the
        specified start sample
    - None if one of the variable's static dimensions has 0 length
    - None if the variable has no dimensions, and sample_start is not
        equal to 0 or sample_count is not equal to 1.
    - None if the first variable dimension is not unlimited, and
        sample_start + sample_count would exceed the dimension length.
    - None if a memory allocation error occurred
    
    """
    cdef void *data = NULL
    data = dsproc_alloc_var_data_index(var.c_ob, sample_start, sample_count)
    if data == NULL:
        return None
    return var.get_datap(sample_start)

def get_var_data_index(cds3.core.Var var):
    """Get a data index for a multi-dimensional variable.
    
    This function will return a data index that can be used to access the data
    in a variable using the traditional x[i][j][k] syntax.  It is up to the
    calling process to cast the return value of this function into the proper
    data type.
    
    Note: If the variable has less than 2 dimensions, the pointer to the
    variable???s data array will be returned.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    var : cds3.core.Var 
        The multi-dimensional variable
    
    Returns
    -------
    - The data index into the variable???s data array
    - None if the pointer to the variable was NULL
    - None if no data has been stored in the variable (var.sample_count == 0)
    - None if a memory allocation error occurs

    """
    cdef void *data = NULL
    data = dsproc_get_var_data_index(var.c_ob)
    if data == NULL:
        return None
    return var.get_datap()

def get_var_data(cds3.core.Var var, CDSDataType cds_type, size_t sample_start,
        size_t sample_count=0, np.ndarray data=None):
    """Get a copy of the data from a dataset variable.

    This function will get the data from a variable casted into the specified
    data type. All missing values used in the data will be converted to a single
    missing value appropriate for the requested data type. The missing value
    used will be the first value returned by cds_get_var_missing_values() if
    that value is within the range of the requested data type, otherwise, the
    default fill value for the requested data type will be used.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
     
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    cds_type : CDSDataType
        Data type of the output missing_value and data array
    sample_start : size_t
        Start sample (0 based indexing)
    
    Returns
    -------
    - The output data array and missing value
    - None if the pointer to the variable was NULL
    - None if the variable has no data for sample_start (sample_count == 0)
    - None if a memory allocation error occurs (sample_count == (size_t)-1)

    """
    cdef void* datap = NULL
    cdef void* missing_c = _alloc_single(cds_type)
    cdef int ndims = var.c_ob.ndims
    cdef size_t sample_size = 0
    cdef np.dtype dtype = cds_type_to_dtype_obj(cds_type)
    if sample_count == 0 or data is None:
        datap = dsproc_get_var_data(var.c_ob, cds_type, sample_start,
                &sample_count, missing_c, NULL)
        if datap == NULL:
            return None,None
        dims = <np.npy_intp*>malloc(sizeof(np.npy_intp) * ndims)
        for i in range(ndims):
            dims[i] = var.c_ob.dims[i].length
        dims[0] = sample_count
        data = np.PyArray_SimpleNewFromData(ndims, dims, dtype.num, datap)
        free(dims)
        # allow numpy to reclaim memory when array goes out of scope
        data.base = PyCapsule_New(datap, NULL, <PyCapsule_Destructor>_free)
        return data, _convert_single(cds_type, missing_c)
    else:
        sample_size = cds_var_sample_size(var.c_ob)
        assert data.flags['C_CONTIGUOUS']
        assert data.size >= sample_count*sample_size
        datap = dsproc_get_var_data(var.c_ob, cds_type, sample_start,
                &sample_count, missing_c, data.data)
        if datap == NULL:
            return None,None
        return data, _convert_single(cds_type, missing_c)

def get_var_missing_values(cds3.core.Var var):
    """Get the missing values for a CDS Variable.

    This function will return an array containing all values specified by
    the missing_value and _FillValue attributes (in that order), and will
    be the same data type as the variable. If the _FillValue attribute does
    not exist but a default fill value has been defined, it will be used
    instead.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    values : output
        Pointer to the array of missing values, the
        data type of this array will be the same as the variable
    
    Returns
    -------
    - Number of missing values
    - 0 if there are no missing or fill values
    - -1 if a memory allocation error occurs

    """
    cdef int retval
    cdef np.npy_intp size
    cdef void *values
    cdef CDSDataType cds_type = var.c_ob.type
    cdef np.ndarray array
    retval = dsproc_get_var_missing_values(var.c_ob, &values)
    if retval < 0:
        return None
    elif retval == 0:
        return np.array([]) # empty, size 0 array of some type
    else:
        size = retval
        array = np.PyArray_SimpleNewFromData(1, &size,
                cds_type_to_dtype(cds_type), values)
        # allow numpy to reclaim memory when array goes out of scope
        array.base = PyCapsule_New(values, NULL, <PyCapsule_Destructor>_free)
        return array

def init_var_data(
        cds3.core.Var var,
        size_t sample_start,
        size_t sample_count,
        bint use_missing):
    """Initialize the data values for a dataset variable.
    
    This function will make sure enough memory is allocated for the specified
    samples and initializing the data values to either the variable's missing
    value (use_missing == True), or 0 (use_missing == False).
    
    The search order for missing values is:
    
    - missing_value attribute
    - _FillValue attribute
    - variable's default missing value
    
    If the variable does not have any missing or fill values defined the
    default fill value for the variable's data type will be used and the
    default fill value for the variable will be set.
    
    If the specified start sample is greater than the variable's current sample
    count, the hole between the two will be filled with the first missing or
    fill value defined for the variable.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.

    Parameters
    ----------
    var : cds3.core.Var
        Variable that will have its data values initialized.
    sample_start : size_t
        Start sample of the data to initialize (0 based indexing).
    sample_count : size_t
        Number of samples to initialize.
    use_missing : bool
        Flag indicating if the variables missing value should be used
        (True == TRUE, False == fill with zeros).

    Returns
    -------
    - Specified start sample in the variable's data array.
    - None if the specified sample count is zero.
    - None if one of the variable's static dimensions has 0 length.
    - None if the variable has no dimensions, and sample_start is not
      equal to 0 or sample_count is not equal to 1.
    - None if the first variable dimension is not unlimited, and
      sample_start + sample_count would exceed the dimension length.
    - None if a memory allocation error occurred.

    """
    cdef void *data = NULL
    data = dsproc_init_var_data(var.c_ob, sample_start, sample_count,
            use_missing)
    if data == NULL:
        return None
    return var.get_datap(sample_start)

def init_var_data_index(
            cds3.core.Var var,
            size_t sample_start,
            size_t sample_count,
            int use_missing):
    """Initialize the data values for a dataset variable.
    
    This function will make sure enough memory is allocated for the specified
    samples and initializing the data values to either the variable's missing
    value (use_missing == 1), or 0 (use_missing == 0).
    
    The search order for missing values is:
    
    - missing_value attribute
    - _FillValue attribute
    - variable's default missing value
    
    If the variable does not have any missing or fill values defined the
    default fill value for the variable's data type will be used and the
    default fill value for the variable will be set.
    
    If the specified start sample is greater than the variable's current sample
    count, the hole between the two will be filled with the first missing or
    fill value defined for the variable.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.

    Parameters
    ----------
    var : cds3.core.Var
        Variable that will have its data values initialized.
    sample_start : size_t
        Start sample of the data to initialize (0 based indexing).
    sample_count : size_t
        Number of samples to initialize.
    use_missing : int
        Flag indicating if the variables missing value should be used
        (1 == TRUE, 0 == fill with zeros).

    Returns
    -------
    - Specified start sample in the variable's data array.
    - None if the specified sample count is zero.
    - None if one of the variable's static dimensions has 0 length.
    - None if the variable has no dimensions, and sample_start is not
      equal to 0 or sample_count is not equal to 1.
    - None if the first variable dimension is not unlimited, and
      sample_start + sample_count would exceed the dimension length.
    - None if a memory allocation error occurred.

    """
    cdef void *data = NULL
    data = dsproc_init_var_data_index(var.c_ob, sample_start, sample_count,
            use_missing)
    if data == NULL:
        return None
    return var.get_datap(sample_start)


def set_var_data(
            cds3.core.Var var,
            size_t sample_start,
            size_t sample_count,
            object missing_value,
            np.ndarray data_nd):
    """Set the data values for a dataset variable.
    
    This function will set the data values of a variable by casting the values
    in the input data array into the data type of the variable. All missing
    values in the input data array will be converted to the first missing value
    used by the variable as returned by cds_get_var_missing_values(). If the
    variable does not have a missing_value or _FillValue attribute defined, the
    default fill value for the variable's data type will be used.
    
    For multi-dimensional variables, the specified data array must be stored
    linearly in memory with the last dimension varying the fastest.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.

    Parameters
    ----------
    var : cds3.core.Var
        Variable to set.
    sample_start : size_t
        Start sample of the new data (0 based indexing).
    sample_count : size_t
        Number of new samples.
    missing_value : object
        Missing value used in the data array, or None if the data does
        not contain any missing values.
    data : np.ndarray
        Input data array.

    Returns
    -------
    - The specified start sample in the variable's data array if successful.
    - None if one of the variable's static dimensions has 0 length
    - None if the variable has no dimensions, and sample_start is not
      equal to 0 or sample_count is not equal to 1.
    - None if the first variable dimension is not unlimited, and
      sample_start + sample_count would exceed the dimension length.
    - None if a memory allocation error occurred

    """
    cdef void *data = NULL
    cdef CDSDataType cds_type
    cdef void *missing_c = NULL
    cds_type = <CDSDataType>dtype_to_cds_type(data_nd.dtype)
    if missing_value is not None:
        missing_c = _alloc_single(cds_type, missing_value)
    data = dsproc_set_var_data(var.c_ob, cds_type, sample_start, sample_count,
            missing_c, data_nd.data)
    if missing_value is not None:
        free(missing_c)
    if data == NULL:
        return None
    return var.get_datap(sample_start)

def get_base_time(cds3.core.Object cds_object):
    """Get the base time of a dataset or time variable.
    
    This function will convert the units attribute of a time variable to
    seconds since 1970. If the input object is a CDSGroup, the specified
    dataset and then its parent datasets will be searched until a "time"
    or "time_offset" variable is found.
    
    Parameters
    ----------
    cds_object : cds3.core.Object
        A CDSGroup or CDSVar
    
    Returns
    -------
    - Base time in seconds since 1970 UTC
    - 0 if not found

    """
    return dsproc_get_base_time(cds_object.cds_object)

#size_t  dsproc_get_time_range(
#            void      *cds_object,
#            timeval_t *start_time,
#            timeval_t *end_time)
def get_time_range(cds3.core.Object cds_object):
    """Get the time range of a dataset or time variable.
    
    This function will get the start and end times of a time variable.
    If the input object is a CDSGroup, the specified dataset and then
    its parent datasets will be searched until a "time" or "time_offset"
    variable is found.
    
    Parameters
    ----------
    cds_object : cds3.core.Object
        A CDSGroup or CDSVar
    start_time : timeval_t
        pointer to the timeval_t structure to store the
        start time in.
    end_time : timeval_t
        pointer to the timeval_t structure to store the
        end time in.
    
    Returns
    -------
    - Number of time values, start=(sec,ms), end=(sec,ms)
    - Empty list if no time values were found

    """
    cdef timeval_t start
    cdef timeval_t end
    cdef size_t num_times
    num_times = dsproc_get_time_range(cds_object.cds_object, &start, &end)
    return (num_times,
            float(start.tv_sec) + float(start.tv_usec)/1000000,
            float(end.tv_sec) + float(end.tv_usec) / 1000000)
#    return num_times, (start.tv_sec, start.tv_usec), (end.tv_sec, end.tv_usec)

def get_time_var(cds3.core.Object cds_object):
    """Get the time variable used by a dataset.
    
    This function will find the first dataset that contains either the "time"
    or "time_offset" variable and return a pointer to that variable.
    
    Parameters
    ----------
    cds_object : cds3.core.Object
        Pointer to a CDSGroup or CDSVar
    
    Returns
    -------
    - The pointer to the first dataset with a time variable
    - None if not found

    """
    cdef CDSVar *cds_var
    cdef cds3.core.Var var
    cds_var = dsproc_get_time_var(cds_object.cds_object)
    if cds_var == NULL:
        return None
    var = cds3.core.Var()
    var.set_var(cds_var)
    return var

def get_sample_times(cds3.core.Object cds_object, size_t sample_start):
    """Get the sample times for a dataset or time variable.
    
    This function will convert the data values of a time variable to seconds
    since 1970. If the input object is a CDSGroup, the specified dataset and
    then its parent datasets will be searched until a "time" or "time_offset"
    variable is found.
    
    Note: If the sample times can have fractional seconds the
    dsproc_get_sample_timevals() function should be used instead.
    
    If an error occurs in this function it will be appended to the log
    
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    cds_object : cds3.core.Object
        Pointer to a CDSGroup or CDSVar
    sample_start : size_t
        Start sample (0 based indexing)
    
    Returns
    -------
    - List of sample times in seconds since 1970 
    - None if there is no data for sample_start (sample_count == 0)
    - None if an error occurred (sample_count == (size_t)-1)

    """
    cdef list retval
    cdef time_t *sample_times = NULL
    cdef size_t sample_count = 0
    sample_times = dsproc_get_sample_times(cds_object.cds_object,
            sample_start, &sample_count, NULL)
    if sample_times == NULL:
        return None
    retval = [long(sample_times[i]) for i in range(sample_count)]
    free(sample_times)
    return retval

#timeval_t *dsproc_get_sample_timevals(
#            void      *cds_object,
#            size_t     sample_start,
#            size_t    *sample_count,
#            timeval_t *sample_times)
def get_sample_timevals(
            cds3.core.Object cds_object,
            size_t sample_start):
            #size_t *sample_count, in/out
            #timeval_t *sample_times) out
    """Get the sample times for a dataset or time variable.
    
    This function will convert the data values of a time variable to an
    array of timeval_t structures. If the input object is a CDSGroup, the
    specified dataset and then its parent datasets will be searched until
    a "time" or "time_offset" variable is found.
    
    Note: Consider using the cds.get_sample_times() function if the sample
    times can not have fractional seconds.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    cds_object : cds3.core.Object
        Pointer to a CDSGroup or CDSVar
    sample_start : size_t
        Start sample (0 based indexing)
    
    Returns
    -------
    - List of sample times in seconds since 1970
    - None if the variable has no data for sample_start (sample_count == 0)
    - None if an error occurred (sample_count == (size_t)-1)

    """
    cdef list retval
    cdef timeval_t *sample_times = NULL
    cdef size_t sample_count = 0
    sample_times = dsproc_get_sample_timevals(cds_object.cds_object,
            sample_start, &sample_count, NULL)
    if sample_times == NULL:
        return None
    retval = [float(sample_times[i].tv_sec) + float(sample_times[i].tv_usec) / 1000000
            for i in range(sample_count)]
    free(sample_times)
    return retval

def fetch_timevals(
            int ds_id,
            object begin,
            object end):
    """Fetch the times of previously stored data.

    This function will retrieve the times of previously stored data for the 
    specified datastream and time range.

    If the begin_timeval is None, the time just prior to the 
    end_timeval will be returned.

    If the end_timeval is None, the time just after the 
    begin_timeval will be returned.

    Parameters
    ----------
    ds_id : int
        Output datastream ID
    begin : object
        beginning of the time range to search
    end : object
        end of the time range to search
    
    Returns
    -------
    - List of time values found in specified range
    - Zero if no times were found in specified range (ntimevals == 0)
    - None if an error occurred (ntimevals == (size_t)-1)

    """
    cdef list retval
    cdef timeval_t *begin_c = NULL
    cdef timeval_t *end_c = NULL
    cdef timeval_t *timevals = NULL
    cdef size_t ntimevals = 0

    # had to declare begin and end as objects to allow them to be None
    # as objects have to convert to double 
    # cython doesn't recognize timeval_t so have to convert to that here
    if begin is not None: 
        d_begin = <double>begin
        begin_c = <timeval_t*>malloc(sizeof(timeval_t) * 1)
        begin_c.tv_sec = modf(d_begin)[1]
        begin_c.tv_usec = modf(d_begin)[0] * 1000000

    if end is not None: 
        d_end = <double>end
        end_c = <timeval_t*>malloc(sizeof(timeval_t) * 1)
        end_c.tv_sec = modf(d_end)[1]
        end_c.tv_usec = modf(d_end)[0] * 1000000

    if begin is not None and end is not None: 
        timevals = dsproc_fetch_timevals(ds_id,
            begin_c, end_c, &ntimevals, NULL)
        free(begin_c)
        free(end_c)
    elif begin is not None:
        timevals = dsproc_fetch_timevals(ds_id,
            begin_c, NULL, &ntimevals, NULL)
        free(begin_c)
    elif end is not None:
        timevals = dsproc_fetch_timevals(ds_id,
            NULL, end_c, &ntimevals, NULL)
        free(end_c)
    else:
        timevals = dsproc_fetch_timevals(ds_id,
            NULL, NULL, &ntimevals, NULL)

    if ntimevals == 0:
        return 0
    if timevals == NULL:
        return None
    return ([float(timevals[i].tv_sec) + float(timevals[i].tv_usec) / 1000000
            for i in range(ntimevals)])

def set_base_time(cds3.core.Object cds_obj, object long_name, size_t base_time):
    """Set the base time of a dataset or time variable.

    This function will set the base time for a time variable and 
    adjust all attributes and data values as necessary. If the 
    input object is one of the standard time variables 
    ("time", "time_offset", or "base_time"), all standard time variables 
    that exist in its parent dataset will also be updated. If the input 
    object is a CDSGroup, the specified dataset and then its parent datasets 
    will be searched until a "time" or "time_offset" variable is found. 
    All standard time variables that exist in this dataset will then be updated.

    For the base_time variable the data value will be set and the "string" 
    attribute will be set to the string representation of the base_time value. 
    The "long_name" and "units" attributes will also be set to "Base time in Epoch" 
    and "seconds since 1970-1-1 0:00:00 0:00" respectively.

    For the time_offset variable the "units" attribute will set to the 
    string representation of the base_time value, and the "long_name" 
    attribute will be set to "Time offset from base_time".

    For all other time variables the "units" attribute will be set to the 
    string representation of the base_time value, and the "long_name" 
    attribute will be set to the specified value. If a long_name attribute 
    is not specified, the string "Time offset from midnight" will be used 
    for base times of midnight, and "Sample times" will be used for all other base times.

    Any existing data in a time variable will also be adjusted for the new base_time value.

    If an error occurs in this function it will be appended to the log and 
    error mail messages, and the process status will be set appropriately.

    Parameters
    ----------
    cds_object : cds3.core.Object
        CDSGroup or CDSVar to set time for.
    long_name : string
        String to use for the long_name attribute, or None to use the default value
    base_time : size_t
        Base time in seconds since 1970.

    Returns
    -------
    int
        1 if successful, 0 if an error occurred.

    """
    cdef int retval
    cdef object b_long_name = None

    #Take into account if long_name is None
    if long_name is None:
        retval = dsproc_set_base_time(cds_obj.cds_object,NULL, base_time)
    else:
        b_long_name = _to_byte_c_string( long_name )
        retval = dsproc_set_base_time(cds_obj.cds_object, b_long_name, base_time)
    return retval

def set_sample_times(cds3.core.Object obj, size_t sample_start,
        object sample_times):
    cdef int retval
    cdef time_t *sample_times_c = NULL
    cdef size_t sample_count = len(sample_times)
    sample_times_c = <time_t*>malloc(sizeof(time_t) * sample_count)
    for i in range(len(sample_times)):
        sample_times_c[i] = sample_times[i]
    retval = dsproc_set_sample_times(<void*>obj.cds_object,
            sample_start, sample_count, sample_times_c)
    free(sample_times_c)
    return retval

def set_sample_timevals(cds3.core.Object obj, size_t sample_start,
        object sample_times):
    """Set the sample times for a dataset or time variable.
    
    This function will set the data values for a time variable by subtracting
    the base time (as defined by the units attribute) and converting the
    remainder to the data type of the variable.
    
    If the input object is one of the standard time variables:
    
    - time
    - time_offset
    - base_time
    
    All standard time variables that exist in its parent dataset will also be
    updated.
    
    If the input object is a CDSGroup, the specified dataset and then its parent
    datasets will be searched until a "time" or "time_offset" variable is found.
    All standard time variables that exist in this dataset will then be updated.
    
    If the specified sample_start value is 0 and a base time value has not
    already been set, the base time will be set using the time of midnight
    just prior to the first sample time.
    
    The data type of the time variable(s) must be either CDS_FLOAT or
    or CDS_DOUBLE. However, CDS_DOUBLE is usually recommended.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    cds_object : cds3.core.Object
        Pointer to a CDSGroup or CDSVar
    sample_start : size_t
        Start sample (0 based indexing)
    sample_count : 
        Number of samples in the times array
    sample_times : object
        Pointer to the array of sample times
        in seconds since 1970 UTC.
    
    Returns
    -------
    - 1 if successful
    - 0 if an error occurred

    """
    cdef int retval
    cdef timeval_t *sample_times_c = NULL
    cdef size_t sample_count = len(sample_times)
    sample_times_c = <timeval_t*>malloc(sizeof(timeval_t) * sample_count)
    for i in range(len(sample_times)):
        sample_times_c[i].tv_sec = modf(sample_times[i])[1]
        sample_times_c[i].tv_usec = modf(sample_times[i])[0] * 1000000
        retval = dsproc_set_sample_timevals(obj.cds_object,
            sample_start, sample_count, sample_times_c)
    free(sample_times_c)
    return retval

def add_var_output_target(cds3.core.Var var, int ds_id, object var_name):
    """Add an output target for a variable.
    
    This function will add an output target for the variable.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    ds_id : int
        Output datastream ID
    var_name : object
        Name of the variable in the output datastream
    
    Returns
    -------
    - 1 if successful
    - 0 if an error occurred

    """
    cdef object b_var_name = _to_byte_c_string( var_name )
    return dsproc_add_var_output_target(var.c_ob, ds_id, b_var_name)

def copy_var_tag(cds3.core.Var src_var, cds3.core.Var dest_var):
    """Copy a variable tag from one variable to another.
    
    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.
    
    Parameters
    ----------
    src_var : cds3.core.Var
        Pointer to the source variable
    dest_var : cds3.core.Var
        Pointer to the destination variable
    
    Returns
    -------
    - 1 if successful
    - 0 if an error occurred

    """
    return dsproc_copy_var_tag(src_var.c_ob, dest_var.c_ob)

def delete_var_tag(cds3.core.Var var):
    """Delete a variable tag.
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable

    """
    dsproc_delete_var_tag(var.c_ob)

def get_source_ds_id(cds3.core.Var var):
    """"Get the ID of the input datastream the variable was retrieved from

    Parameters
    ----------
    var : cds3.core.Var
        Variable whose source id will be retrieved.

    Returns
    -------
    int
        - Datastream ID of the variable's source.
        - -1 if the variable was not explicitly requested by user in
          the retriever definition.

    """
    cdef int retval
    retval = dsproc_get_source_ds_id(var.c_ob)
    return retval

def get_source_var_name(cds3.core.Var var):
    """"Get the name of the source variable read in from the input file.

    The memory used by the returned variable name belongs to the internal
    variable tag and must not be freed or altered by the calling process.

    Parameters
    ----------
    var : cds3.core.Var
        Variable that the name will be retrieved for.

    Returns
    -------
    - Name of the variable's source.
    - None if the variable was not explicitly requested by user in
      the retriever definition.

    """
    cdef const char *retval
    retval = dsproc_get_source_var_name(var.c_ob)
    if retval == NULL:
        return None
    return _to_python_string(retval)

def get_source_ds_name(cds3.core.Var var):
    """"Get the name of the input datastream the variable was retrieved 
    from.

    The memory used by the returned variable name belongs to the internal
    variable tag and must not be freed or altered by the calling process.

    Parameters
    ----------
    var : cds3.core.Var
        Variable that the datastream name will be retrieved for.

    Returns
    -------
    - Datastream name of the variable's source.
    - None if the variable was not explicitly requested by user in
      the retriever definition.

    """
    cdef const char *retval
    retval = dsproc_get_source_ds_name(var.c_ob)
    if retval == NULL:
        return None
    return _to_python_string(retval)


#int     dsproc_get_var_output_targets(
#            CDSVar      *var,
#            VarTarget ***targets)
def get_var_output_targets(cds3.core.Var var):
    """Get the output targets defined for the specified variable.
    
    The memory used by the output array belongs to the internal variable
    tag and must not be freed or altered by the calling process.

    Parameters
    ----------    
    var : cds3.core.Var
        Pointer to the variable
    targets :  output
        List of Pointers to the VarTarget structures.
        The ds_id member of VarTarget is a property of returned list items
        The var_name of VarTarget is a property of returned list items
     
    
    Returns
    -------
    - A list of output variable targets
    - 0 if no variable targets have been defined

    """
    cdef VarTarget **targets
    cdef int count
    cdef list retval = []
    cdef PyVarTarget target
    count = dsproc_get_var_output_targets(var.c_ob, &targets)
    for i in range(count):
        target = PyVarTarget()
        target.set_vartarget(targets[i])
        retval.append(target)
    return retval

def set_var_coordsys_name(cds3.core.Var var, object coordsys_name):
    cdef object b_coordsys_name = _to_byte_c_string( coordsys_name )
    return dsproc_set_var_coordsys_name(var.c_ob, b_coordsys_name)

def set_var_flags(cds3.core.Var var, int flags):
    return dsproc_set_var_flags(var.c_ob, flags)

def set_var_output_target(cds3.core.Var var, int ds_id, object var_name):
    cdef object b_var_name = _to_byte_c_string( var_name )
    return dsproc_set_var_output_target(var.c_ob, ds_id, b_var_name)

def unset_var_flags(cds3.core.Var var, int flags):
    '''
    Unset the control flags for a variable.

    See dsproc_set_var_flags() for flags and descriptions

    Parameters
    ----------
    var : cds3.core.Var
        Variable
    flags : int
        Flags to set
    '''
    dsproc_unset_var_flags(var.c_ob, flags)

#ctypedef struct VarDQR:
#    char  *id          # DQR ID
#    char  *desc        # description
#    char  *ds_name     # datastream name
#    char  *var_name    # variable name
#    int          code        # code number
#    char  *color       # code color
#    char  *code_desc   # code description
#    time_t       start_time  # start time in seconds since 1970
#    time_t       end_time    # end time in seconds since 1970
#    size_t       start_index # start time index in dataset
#    size_t       end_index   # end time index in dataset
#
#int dsproc_get_var_dqrs(CDSVar *var, VarDQR ***dqrs)

def dump_dataset(
        cds3.core.Group dataset,
        object outdir,
        object prefix,
        time_t file_time,
        object suffix,
        int flags):

    cdef object b_outdir = _to_byte_c_string( outdir )
    cdef object b_prefix = _to_byte_c_string( prefix )
    cdef object b_suffix = _to_byte_c_string( suffix )

    return dsproc_dump_dataset(dataset.c_ob, b_outdir, b_prefix, file_time, 
            b_suffix, flags)

def dump_output_datasets(object outdir, object suffix, int flags):
    cdef object b_outdir = _to_byte_c_string( outdir )
    cdef object b_suffix = _to_byte_c_string( suffix )
    return dsproc_dump_output_datasets(b_outdir, b_suffix, flags)

def dump_retrieved_datasets(object outdir, object suffix, int flags):
    cdef object b_outdir = _to_byte_c_string( outdir )
    cdef object b_suffix = _to_byte_c_string( suffix )
    return dsproc_dump_retrieved_datasets(b_outdir, b_suffix, flags)

def dump_transformed_datasets(object outdir, object suffix, int flags):
    cdef object b_outdir = _to_byte_c_string( outdir )
    cdef object b_suffix = _to_byte_c_string( suffix )
    return dsproc_dump_transformed_datasets(b_outdir, b_suffix, flags)

def copy_file(object src_file, object dest_file):
    cdef object b_src_file = _to_byte_c_string( src_file )
    cdef object b_dest_file = _to_byte_c_string( dest_file )
    return dsproc_copy_file(b_src_file, b_dest_file)

def move_file(object src_file, object dest_file):
    cdef object b_src_file = _to_byte_c_string( src_file )
    cdef object b_dest_file = _to_byte_c_string( dest_file )
    return dsproc_move_file(b_src_file, b_dest_file)

def execvp(object infile, object inargv, int flags):
    cdef char **c_inargv = <char **>malloc(len(inargv) * sizeof(char *))
    cdef object b_inargv = [None] * len(inargv)
    return_value = 1
    # String processing varies depending on which Python version is run
    # The PyString_AsString is different with different functionality
    # depending on the Python version. See imports.
    if sys.version_info[0] < 3:
        # Python Major Version 2
        for i in range(len(inargv)):
            if i < len(inargv)-1:
                c_inargv[i] = PyString_AsString(inargv[i])
            if i == len(inargv)-1:
                c_inargv[i] = NULL
    else:
        # Python Major Version 3
        for i in range(len(inargv)):
            if i < len(inargv)-1:
                b_inargv[i] = PyUnicode_AsEncodedString(inargv[i], "UTF-8","strict")
            if i < len(inargv)-1:
                c_inargv[i] = PyString_AsString(b_inargv[i])
            if i == len(inargv)-1:
                c_inargv[i] = NULL

    b_infile = _to_byte_c_string(infile)

    return_value = dsproc_execvp(b_infile, c_inargv, flags)
    free(c_inargv)
    del b_inargv
    return return_value

def create_timestamp(time_t secs1970):
    cdef char timestamp[16]

    """Convert seconds since 1970 to a timestamp.
    This function will create a timestamp of the form:
    'YYYYMMDD.hhmmss'

    The timestamp argument must be large enough to hold
    at least 16 characters (15 plus the null terminator).

    If an error occurs in this function it will be
    appended to the log and error mail messages, and
    the process status will be set appropriately.

    Parameters
    ----------
    secs1970 : time_t
        Seconds since 1970
    timestamp : output
        Timestamp string in the form YYYYMMDD.hhmmss

    Returns
    -------
    - 1 if successful
    - 0 if an error occurred
    """

    return_value = dsproc_create_timestamp(secs1970, timestamp)
    if return_value == 0:
        return None
    else:
        return _to_python_string( timestamp )

#FILE   *dsproc_open_file(char *file)
#

def run_dq_inspector(int ds_id, time_t begin_time,
       time_t end_time, object input_args, int flag):
    """
    Run DQ Inspector.
    This function will run dq_inspector for the specified 
    datastream and time range. The following arguments will 
    be automatically added to the dq_inspector command 
    line so they do not need to be specified by the 
    calling process in the args[] array:

    -P
    -r read_path
    -d datastream
    -s start_date
    -e end date

    If an error occurs in this function it will be 
    appended to the log and error mail messages, 
    and the process status will be set appropriately.

    A warning message will also be generated if 
    dq_inpsector returns a non-zero exit value;
   
    Parameters
    ----------
    ds_id : int
        Datastream ID
    begin_time : time_t
        Beginning of the time range to search
    end_time : time_t
        End of the time range to search
    input_args : object
        List of command line arguments for dq_inspector.
    flag : int
        Control flag.  Set to 0 to maintain backward 
        compatibility.

    Returns
    -------
    - exit value (0 == success)
    - -1  if the process could not be executed

    """
    
    if len(input_args) != 0 and input_args[-1] != None:
        input_args = input_args + [None]

    cdef int return_value

    cdef const char **c_input_args = <const_char**>malloc(len(input_args) * sizeof(char*))

    cdef object b = None

    # String processing varies depending on which Python version is run
    # The PyString_AsString is different with different functionality depending on the Python version. See imports.
    if sys.version_info[0] < 3:
        # Python Major Version 2
        for i in range(len(input_args)):
            if i == len(input_args)-1:
                c_input_args[i] = NULL
            else:
                c_input_args[i] = PyString_AsString(input_args[i])
    else:
        # Python Major Version 3
        for i in range(len(input_args)):
            if i == len(input_args) - 1:
                c_input_args[i] = NULL
                break
            b = PyUnicode_AsEncodedString(input_args[i], "UTF-8", "strict")
            c_input_args[i] = PyString_AsString(b)
    
    return_value = dsproc_run_dq_inspector(ds_id, begin_time, end_time, c_input_args, flag)
    free(c_input_args)
    del b
    return return_value

def add_datastream_file_patterns(int ds_id, object patterns, int ignore_case):
    """
    Adds file patterns to look for when creating the list of 
    files in the datastream directory.  By default all files in the directory
    will be listed. 
    If an error occurs, it will be appened to the log and error mail messages,
    and the process status will be set appropriately.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    patterns : object
        List of extened regex file patterns (man regcomp)
    ignore_case : int
        Ignore case in file_patterns 
    """
    cdef int return_value

    cdef const char **c_patterns = <const_char**>malloc(len(patterns) * sizeof(char*))

    cdef object b_patterns = [None] * len(patterns)

    # String processing varies depending on which Python version is run
    # The PyString_AsString is different with different functionality depending on the Python version. See imports.
    if sys.version_info[0] < 3:
        # Python Major Version 2
        for i in range(len(patterns)):
            c_patterns[i] = PyString_AsString(patterns[i])
    else:
        # Python Major Version 3
        for i in range(len(patterns)):
            b_patterns[i] = PyUnicode_AsEncodedString(patterns[i], "UTF-8","strict")

        for i in range(len(patterns)):
            c_patterns[i] = PyString_AsString(b_patterns[i])

    return_value = dsproc_add_datastream_file_patterns(ds_id, len(patterns),
            c_patterns, ignore_case)
    free(c_patterns)
    del b_patterns
    return return_value

def set_file_name_time_patterns(int ds_id, object patterns):
    """
    Set the file name time pattern(s) used to parse the time from a file name.

    The file name time pattern(s) will also be used to sort the list of files in the datastream directory. 
    Alternatively a file_name_compare function can be specified using dsproc_set_file_name_compare_function() (not implemented in Python), 
    or a file_name_time function can be specified using dsproc_set_file_name_time_function()  (not implemented in Python). 
    If more than one are specified the order of precedence is:

    ```
    file_name_compare function
    file name time patterns
    file_name_time function
    ```

    The file name time pattern(s) contain a mixture of regex (see regex(7)) and time format codes similar to the 
    strptime function. The time format codes recognized by this function begin with a % and are followed by one 
    of the following characters:

    * 'C' century number (year/100) as a 2-digit integer
    * 'd' day number in the month (1-31).
    * 'e' day number in the month (1-31).
    * 'h' hour * 100 + minute (0-2359)
    * 'H' hour (0-23)
    * 'j' day number in the year (1-366).
    * 'm' month number (1-12)
    * 'M' minute (0-59)
    * 'n' arbitrary whitespace
    * 'o' time offset in seconds
    * 'p' AM or PM
    * 'q' Mac-Time: seconds since 1904-01-01 00:00:00 +0000 (UTC)
    * 's' seconds since Epoch, 1970-01-01 00:00:00 +0000 (UTC)
    * 'S' second (0-60; 60 may occur for leap seconds)
    * 't' arbitrary whitespace
    * 'y' year within century (0-99)
    * 'Y' year with century as a 4-digit integer
    * '' a literal "%" character

    An optional 0 character can be used between the % and format code to specify that the number must 
    be zero padded. For example, '%0d' specifies that the day range is 01 to 31.

    Multiple patterns can be provided and will be checked in the specified order.

    Examples:

    * "%Y%0m%0d\\.%0H%0M%0S\\.[a-z]$" would match *20150923.072316.csv
    * "%Y-%0m-%0d_%0H:%0M:%0S\\.dat" would match *2015-09-23_07:23:16.dat
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    patterns : object
        List of extened regex time patterns

    Returns
    -------
    - 1 if successful
    - 0 if a regex compile error occurred
    """
    cdef int return_value

    cdef const char **c_patterns = <const_char**>malloc(len(patterns) * sizeof(char*))

    cdef object b_patterns = [None] * len(patterns)

    # String processing varies depending on which Python version is run
    # The PyString_AsString is different with different functionality depending on the Python version. See imports.
    if sys.version_info[0] < 3:
        # Python Major Version 2
        for i in range(len(patterns)):
            c_patterns[i] = PyString_AsString(patterns[i])
    else:
        # Python Major Version 3
        for i in range(len(patterns)):
            b_patterns[i] = PyUnicode_AsEncodedString(patterns[i], "UTF-8","strict")

        for i in range(len(patterns)):
            c_patterns[i] = PyString_AsString(b_patterns[i])

    return_value = dsproc_set_file_name_time_patterns(ds_id, len(patterns),
            c_patterns)
    free(c_patterns)
    del b_patterns
    return return_value

def set_datastream_file_extension(int ds_id, object extension):
    """
    Set the datastream file extension.

    Parameters
    ----------
    ds_id : int
        Datastream ID
    extenstion : object
        File extension
    """
    cdef object b_extension = _to_byte_c_string( extension )
    dsproc_set_datastream_file_extension(ds_id, b_extension)

def get_datastream_files(int ds_id):
    cdef int count
    cdef char **file_list = NULL
    list = []
    count = dsproc_get_datastream_files(ds_id, &file_list)
    for i in range(count):
        list.append( _to_python_string(file_list[i]) )
    return list

def find_datastream_files(int ds_id, time_t begin_time,
       time_t end_time):
    """
    Find all files in a datastream directory for a specified time range.
    This function will return a list of all files in a datastream
    directory containing data for the specified time range.
    This search will include the begin_time but exclude the end_time.
    That is, it will find files that include data greter than or
    equal to the begin time, and less than the end time.

    If the begin_time is not specified, the file containing data for
    the time just prior to the end_time will be returned.

    If the end_time is not specified, the file containing data for
    the time just after the begin_time will be returned.

    The memory used by the returned file list is dynamically allocated
    and must be freed using the dsproc_free_file_list() function.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    begin_time : time_t
        Beginning of the time range to search
    end_time : time_t
        End of the time range to search
    file_list : output
        Pointer to the NULL terminated file list

    Returns
    -------
    - file_list

    """
    cdef int count
    cdef char **file_list = NULL
    list = []
    count = dsproc_find_datastream_files(ds_id, begin_time,
       end_time, &file_list)
    for i in range(count):
        list.append( _to_python_string(file_list[i]) )
    return list

def set_datastream_split_mode(int ds_id, enums.SplitMode split_mode, 
        double split_start, double split_interval):
    """
    Set the file splitting mode for output files
    Default for VAPs:  always create a new file when data is stored
    split_mode = SPLIT_ON_STORE
    split_start = ignored
    split_interval = ignored
    Default for ingests: daily files that split at midnight
    split_mode = SPLIT_ON_HOURS
    split_start = 0
    split_interval = 24

    Parameters
    ----------
    ds_id : int
        Datastream ID
    split_mode : enums.SplitMode
        The file splitting mode (see SplitMode)
    split_interval : double
        The split interval (see Split Mode)
    """
    dsproc_set_datastream_split_mode(ds_id, split_mode, split_start, split_interval)

#    dsproc_set_file_name_compare_function(ds_id, function)

def rename(
        int ds_id,
        object file_path,
        object file_name,
        time_t begin_time,
        time_t end_time):

    cdef object b_file_path = _to_byte_c_string( file_path)
    cdef object b_file_name = _to_byte_c_string( file_name)

    return dsproc_rename(ds_id, b_file_path, b_file_name, begin_time, end_time)

#int     dsproc_rename_tv(
#            int              ds_id,
#            char      *file_path,
#            char      *file_name,
#            timeval_t *begin_time,
#            timeval_t *end_time)

def rename_bad(int ds_id, object file_path, object file_name, time_t file_time):

    cdef object b_file_path = _to_byte_c_string( file_path)
    cdef object b_file_name = _to_byte_c_string( file_name)

    return dsproc_rename_bad(ds_id, b_file_path, b_file_name, file_time)

def set_rename_preserve_dots(int ds_id, int preserve_dots):
    dsproc_set_rename_preserve_dots(ds_id, preserve_dots)

def set_transform_param(cds3.core.Group group, object name, object param_name, CDSDataType cds_type, size_t length, object value):
    """Set the value of a transformation parameter.

    Error messages from this function are sent to the message handler

    Parameters
    ----------
    group : object
        Pointer to the CDSGroup 
    obj_name : object
        Name of the CDS object
    param_name : object
        param_name
    cds_type : CDSDataType
        type
    length : size_t
        length
    value : void *
        value

    Returns
    -------
    - 1 if the variable was renamed
    - 0 if a variable with the new name already exists
    - 0 if the variable is locked
    - 0 if the group is locked """
    cdef object b_name = _to_byte_c_string(name)
    cdef object b_param_name = _to_byte_c_string(param_name)
    cdef int retval
    cdef np.ndarray value_nd = np.asarray(value, cds_type_to_dtype_obj(cds_type))
    if value_nd.ndim == 0:
        value_nd = value_nd[None] # add dummy dimension to a scalar value
    assert value_nd.ndim == 1
    retval = cds_set_transform_param(group.c_ob, b_name, b_param_name, cds_type, length, value_nd.data)
    return retval

##############################################################################
# ported from dsproc3_internal.h
##############################################################################

def initialize(argv, proc_model, proc_version, proc_names):
    cdef char **c_argv = NULL
    cdef const char **c_proc_names = NULL

    cdef object b_argv = None
    cdef object b_proc_names = None
    cdef object b_proc_version = _to_byte_c_string( proc_version)

    # To pass a list of strings into C for Python3, you must first
    # copy the list into a list of byte strings, and then assign
    # the C array of strings the values from the byte strings list
    #   
    # Attempting to convert an individaul string and then assignign it
    # to the char ** data type does not work. This is because for
    # whatever reason, Python3/Cython 0.23 does not treat the 
    # input variable as in scope for the entire function, and some
    # sort of garbage collection happens that causes the char ** 
    # assigments to incorrectly point to the byte string. It may
    # also be that the converted byte string is treated as temporary,
    # if not assinged to its own variable or list that stays in the
    # same scope as the char ** and could get garbage collected
    # and will only point to the most recent one i.e. not constant
    #
    # Essentially, you need a reference to the python byte string
    # when going to c string, or wont compile
    #
    # Source: http://docs.cython.org/en/latest/src/tutorial/strings.html#encoding-text-to-bytes

    if argv:
        c_argv = <char**>malloc(sizeof(char*) * len(argv))
        if c_argv is NULL:
            raise MemoryError()
   
        #Create byte string version
        b_argv = [None] * len(argv)

        for idx, s in enumerate(argv):
            b_argv[idx] = _to_byte_c_string(s)

        for idx,s in enumerate(b_argv):
            c_argv[idx] = s
    if proc_names:
        c_proc_names = <const_char**>malloc(sizeof(char*) * len(proc_names))
        if c_proc_names is NULL:
            raise MemoryError()

        #Create byte string version
        b_proc_names = [None] * len(proc_names)

        for idx, s in enumerate( proc_names ):
            b_proc_names[idx] = _to_byte_c_string(s)

        for idx,s in enumerate(b_proc_names):
            c_proc_names[idx] = s
    try:
        dsproc_initialize(len(argv), c_argv, proc_model, b_proc_version,
                len(proc_names), c_proc_names)
    finally:
        free(c_argv)
        free(c_proc_names)
        del b_argv
        del b_proc_names

#int dsproc_start_processing_loop(
#        time_t *interval_begin,
#        time_t *interval_end)
#
#int dsproc_retrieve_data(
#        time_t     begin_time,
#        time_t     end_time,
#        CDSGroup **ret_data)
#
#int dsproc_merge_retrieved_data()
#
#int dsproc_transform_data(
#        CDSGroup **trans_data)
#
#int dsproc_create_output_datasets()
#
#int dsproc_store_output_datasets()
#
def finish():
    return dsproc_finish()
#
#void dsproc_debug(
#        char *func,
#        char *file,
#        int         line,
#        int         level,
#        char *format, ...)
#
#void dsproc_error(
#        char *func,
#        char *file,
#        int         line,
#        char *status,
#        char *format, ...)
#
#void dsproc_log(
#        char *func,
#        char *file,
#        int         line,
#        char *format, ...)
#
#void dsproc_mentor_mail(
#        char *func,
#        char *file,
#        int         line,
#        char *format, ...)
#
#void dsproc_warning(
#        char *func,
#        char *file,
#        int         line,
#        char *format, ...)
#void dsproc_disable(char *message)
#void dsproc_disable_db_updates()
#void dsproc_disable_lock_file()
#void dsproc_disable_mail_messages()

def set_processing_interval(time_t begin_time, time_t end_time):
    """
    Set the begin and end times for the current processing interval.
    This function can be used to override the begin and end times of 
    the current processing interval and should be called from the 
    pre-retrieval hook function.
    
    Parameters
    ----------
    begin_time : time_t
        Begin time in seconds since 1970.
    end_time : time_t
        End time in seconds since 1970.
    """
    dsproc_set_processing_interval(begin_time, end_time)

def set_processing_interval_offset(time_t offset):
    """
    Set the offset to apply to the processing interval.
    This function can be used to shift the processing interval
    and should be called from either the init-process or
    pre-retrieval hook function.
    
    Parameters
    ----------
    offset : time_t
        Offset in seconds
    """
    dsproc_set_processing_interval_offset(offset)

def set_datastream_split_tz_offset(int ds_id, int split_tz_offset):
    """
    Set the timezone offset to use when splitting files.

    Note that this should be the timezone offset for loaction of the data
    being processed and is subtracted from the UTC time when determining
    the time of the next file split.  For example, If a timezone offset
    of -6 hours is set for SGP data, the files will split at 6:00 a.m. GMT.

    Args:
        ds_id (int): datastream id
        split_tz_offset (int): time zone offset in hours

    """
    dsproc_set_datastream_split_tz_offset(ds_id, split_tz_offset)

def get_missing_value_bit_flag(object bit_descs) -> int:
    """
    Get bit flag for the missing_value check.

    This function will search for a bit description that begins with one of the following strings:

     - "Value is equal to missing_value"
     - "Value is equal to the missing_value"
     - "value = missing_value"
     - "value == missing_value"

     - "Value is equal to missing value"
     - "Value is equal to the missing value"
     - "value = missing value"
     - "value == missing value"

     - "Value is equal to _FillValue"
     - "Value is equal to the _FillValue"
     - "value = _FillValue"
     - "value == _FillValue"

    Note: Use dsproc_get_qc_bit_descriptions() to get the list of bit descriptions for a QC variable.

    Parameters
    ----------
    bit_descs : List[str]
        List of bit descriptions to search for missing value.

    Returns:
        Bit flag starting from 1 or 0 if not found.
    """

    # We are converting the python List[str] into a char** variable that we can pass to c function
    ndescs = len(bit_descs)
    c_descs = <const_char**> malloc(ndescs * sizeof(char *))
    for idx in range(ndescs):
        byte_c_str = _to_byte_c_string(bit_descs[idx])
        c_descs[idx] = byte_c_str

    return dsproc_get_missing_value_bit_flag(ndescs, c_descs)

def get_qc_bit_descriptions(cds3.core.Var var):
    """
    Return a List[str] containing the bit descriptions for the given QC variable
    """
    # Create an empty char** and pass the address in as a parameter
    cdef const char** c_bit_descs = NULL
    ndescs = dsproc_get_qc_bit_descriptions(var.c_ob, &c_bit_descs)

    if ndescs < 0:
        return []

    else:
        bit_descs: List[str] = []
        for idx in range(ndescs):
            bit_descs.append(_to_python_string(c_bit_descs[idx]))
        free(c_bit_descs)
        return bit_descs

def set_retriever_time_offsets(int ds_id, time_t begin_time, time_t end_time):
    """
    Set the time offsets to use when retrieving data.
    This function can be used to override the begin and end time 
    offsets specified in the retriever definition and should be
    called from the pre-retrieval hook function.
    
    Parameters
    ----------
    ds_id : int
        input datastream ID
    begin_time : time_t
        Begin time in seconds since 1970.
    end_time : time_t
        End time in seconds since 1970.
    """
    dsproc_set_retriever_time_offsets(ds_id, begin_time, end_time)

def set_trans_qc_rollup_flag(int flag):
    """Set the global transformation QC rollup flag.
    
    This function should typically be called from the users 
    init_process function, but must be called before the 
    post-transform hook returns.

    Setting this flag to true specifies that all bad and 
    indeterminate bits in transformation QC variables should 
    be consolidated into a single bad or indeterminate bit when 
    they are mapped to the output datasets. This bit consolidation 
    will only be done if the input and output QC variables have 
    the appropriate bit descriptions:

    The input transformation QC variables will be determined by 
    checking the tag names in the bit description attributes. 
    These must be in same order as the transformation would define them.
    The output QC variables must contain two bit descriptions for the 
    bad and indeterminate bits to use, and these bit descriptions must 
    begin with the following text:

    - "Transformation could not finish"
    - "Transformation resulted in an indeterminate outcome"

    An alternative to calling this function is to set the
    "ROLLUP TRANS QC" flags for the output datastreams and/or 
    retrieved variables. See dsproc_set_datastream_flags() and 
    dsproc_set_var_flags(). These options should not typically be 
    needed, however, because the internal mapping logic will determine 
    when it is appropriate to do the bit consolidation.

    Parameters
    ----------
    flag : int
        Transformation QC rollup flag (1= TRUE, 0=FALSE).

    """
    dsproc_set_trans_qc_rollup_flag(flag)

#char *dsproc_lib_version()


def get_status():
    return _to_python_string( dsproc_get_status() )

def get_force_mode():
    """Get the force mode.
    
    The force mode can be enabled using the -F option on the command line. This mode 
    can be used to force the process past all recoverable errors that would normally stop 
    process execution.
   
    Returns
    ------- 
    - 1 if force_mode is enabled
    - 0 if force_mode is disabled

    """
    cdef int return_value = dsproc_get_force_mode()
    return return_value

#char *dsproc_get_type()
#char *dsproc_get_version()
#
#time_t      dsproc_get_max_run_time()
#time_t      dsproc_get_start_time()
#time_t      dsproc_get_time_remaining()
#time_t      dsproc_get_min_valid_time()
#time_t      dsproc_get_data_interval()
#time_t      dsproc_get_processing_interval(time_t *begin, time_t *end)

def set_status(object status):
    """Set the process status"""
    cdef object b_status = _to_byte_c_string( status )
    dsproc_set_status(b_status)

def init_datastream(
        object site,
        object facility,
        object dsc_name,
        object dsc_level,
        enums.DSRole role,
        object path,
        enums.DSFormat dsformat,
        int flags):
    """Initialize a new datastream.

    If the specified datastream already exists, the ID of the existing datastream will be returned.

    The default datastream path will be set if path = NULL, 
        see dsproc_set_datastream_path() for details.
    The default datastream format will be set if format = 0, 
        see dsproc_set_datastream_format() for details.
    The default datastream flags will be set if flags < 0, 
        see dsproc_set_datastream_flags() for details.

    For output datastreams:
    The datastream DOD information will be loaded from the database.
    The previously processed data times will be loaded from the database 
        (if database updates have not been disabled).
    The preserve_dots value used by the rename functions will be set to 
        the default value (see dsproc_set_rename_preserve_dots()).
    If an error occurs in this function it will be appended to the log 
        and error mail messages, and the process status will be set appropriately.
    
    Parameters
    ---------- 
    site : object
        Site name, or NULL to find first match
    facility : object
        Facility name, or NULL to find first match
    dsc_name : object
        Datastream class name
    dsc_level : object
        Datastream class level
    role : enums.DSRole
        Specifies input or output datastream
    path : object
        Path to the datastream directory 
    dsformat : enums.DSFormat
        Datastream data format
    flags : int
        Control Flags
    
    Returns
    -------
    - Datastream ID
    - -1 if the datastream has not beed defined

    """

    cdef object b_site = _to_byte_c_string(site)
    cdef object b_facility = _to_byte_c_string(facility)
    cdef object b_dsc_name = _to_byte_c_string(dsc_name)
    cdef object b_dsc_level = _to_byte_c_string(dsc_level)
    cdef object b_path = _to_byte_c_string(path)

    return dsproc_init_datastream(b_site, b_facility, b_dsc_name, b_dsc_level, 
                 role, b_path, dsformat, flags)

#void    dsproc_update_datastream_data_stats(
#            int              ds_id,
#            int              num_records,
#            timeval_t *begin_time,
#            timeval_t *end_time)
#
#void    dsproc_update_datastream_file_stats(
#            int              ds_id,
#            double           file_size,
#            timeval_t *begin_time,
#            timeval_t *end_time)
#
#int     dsproc_validate_datastream_data_time(
#            int              ds_id,
#            timeval_t *data_time)
#
def set_datastream_flags(int ds_id, int flags):
    """Set the control flags for a datastream.

    - DS_STANDARD_QC for all 'b' level datastreams
    - DS_FILTER_NANS for all 'a' and 'b' level datastreams
    - DS_OVERLAP_CHECK for all output datastreams

    Control flags:

    - DS_STANDARD_QC = Apply standard QC before storing a dataset.
    - DS_FILTER_NANS = Replace NaN and Inf values with missing values
      before storing a dataset.
    - DS_OVERLAP_CHECK = Check for overlap with previously processed data.
      This flag will be ignored and the overlap check will
      be skipped if reprocessing mode is enabled.
    - DS_PRESERVE_OBS = Preserve distinct observations when retrieving
      and storing data. Only observations that start within
      the current processing interval will be read in.
    - DS_DISABLE_MERGE = Do not merge multiple observations in retrieved data.
      Only data for the current processing interval will be read in.
    - DS_SKIP_TRANSFORM = Skip the transformation logic for all variables in this datastream.
    - DS_ROLLUP_TRANS_QC = Consolidate the transformation QC bits for all variables
      when mapped to teh output datasets.
    - DS_SCAN_MODE = Enable scan mode for datastream that are not expected to be continuous.
      This prevents warning messages from being generated when data is not found within 
      a processing interval. Instead, a message will be written to the log file indicating 
      that the procesing interval was skipped.
    - DS_OBS_LOOP = Loop over observations instead of time intervals. 
      This also sets the DS_PRESERVE_OBS flag.

    Parameters
    ----------
    ds_id : int
        Datastream ID to set flags for.
    flags : int
        Flags to set.

    """
    dsproc_set_datastream_flags(ds_id, flags)

#void    dsproc_set_datastream_format(int ds_id, DSFormat format)
def set_datastream_path(int ds_id, object path):
    """
    Set the path to the datastream direcotry.

    Default datastream path set if path = NULL:
        dsenv_get_collection_dir() for level 0 input datastream
        dsenv_get_datastream_fir() for all other datastremas

    If an error occurs in this function it will be appended to the 
    log and error mail messages, and the process status will be 
    set appropriately.

    Parameters
    ----------
    ds_id : int
        Datastream ID
    path : object
        Path to the datastream directory

    Returns
    -------
    - 1 if successful
    - 0 if an error occurred
    """
    cdef object b_path = _to_byte_c_string( path)
    return(dsproc_set_datastream_path(ds_id, b_path))
#
#void    dsproc_unset_datastream_flags(int ds_id, int flags)
def unset_datastream_flags(int ds_id, int flags):
    """
    Unset the control flags for a datastream.
    See dsproc_set_datastream_flags() for flags and descriptions.
    
    Parameters
    ----------
    ds_id : int
        Datastream ID
    flags : int
        Flags to set
    """
    dsproc_unset_datastream_flags(ds_id, flags)

def create_output_dataset(int ds_id, time_t data_time, int set_location ):

    cdef cds3.core.Group group
    cdef CDSGroup *cds_group = NULL
    cds_group = dsproc_create_output_dataset(ds_id, data_time, set_location)
    if cds_group == NULL:
        return None
    else:
        group = cds3.core.Group()
        group.set_group(cds_group)
        return group

#int     dsproc_dataset_pass_through(
#            CDSGroup *in_cds,
#            CDSGroup *out_cds,
#            int       flags)
#
def map_datasets(cds3.core.Group in_parent, cds3.core.Group out_dataset, int flags):
    """Map data from input datsets to output datasets.
 
    This function maps all input data to output datasets if an output dataset is
    not specified.  By default only the data within the current processing 
    interval will be mapped to the output dataset.  This can be changed using the 
    set_map_time_range() function.  

    If an error occurs an error message will be appended to the log and error mail 
    messages, and the process status will be set appropriately.

    Parameters
    ----------
    in_parent : cds3.core.Group
        The parent group of all input datasets, typically the ret_data or trans_data parents.
    out_dataset :cds3.core.Group
        The output dataset, or None to map all input data to output datsets.
    flags : int
        Reserved for control flags.
    
    Returns
    -------
    int
        1 if successful, 0 if an error occurred

    """
    cdef int status

    status = dsproc_map_datasets(in_parent.c_ob, out_dataset.c_ob, flags)

    return status

def set_map_time_range(time_t begin_time, time_t end_time):
    """Set the time range to use in subsequent calls to map_datasets()

    Parameters
    ----------
    begin_time : time_t
        Only map data whose time is >= this begin time
    end_time : time_t
        Only map data whose time is < this end time

    """
    dsproc_set_map_time_range(begin_time, end_time)


#
#int     dsproc_map_var(
#            CDSVar *in_var,
#            size_t  in_sample_start,
#            size_t  sample_count,
#            CDSVar *out_var,
#            size_t  out_sample_start,
#            int     flags)
#
#int     dsproc_set_dataset_location(CDSGroup *cds)
#
def store_dataset(int ds_id, int newfile):
    """Store the output dataset.

    This function will:

    - Filter out duplicate records in the dataset, and verify that the 
      record times are in chronological order. Duplicate records are
      defined has having identical times and data values.

    - Filter all NaN and Inf values for variables that have a missing
      value defined for datastreams that have the DS_FILTER_NANS flag 
      set. This should only be used if the DS_STANDARD_QC flag is also set,
      or for datasets that do not have any QC variables defined.

    - Apply standard missing value, min, max, and delta QC checks for
      datastreams that have the DS_STANDARD_QC flag set. This is the default
      or b level datastreams. (see the dsproc_set_datastream_flags() function).

    - Filter out all records that are duplicates of previously stored data,
      and verify that the records do not overlap any previously stored data.
      This check is currently being skipped if we are in reprocessing mode, 
      and the file splitting mode is SPLIT_ON_STORE (the default for VAPs).

    - Verify that none of the record times are in the future.

    - Merge datasets with existing files and only split on defined intervals 
      or when metadata values change. The default for VAPs is to create a
      new file for every dataset stored, and the default for ingests is to
      create daily files that split at midnight UTC (see the 
      dsproc_set_datastream_split_mode() function). 

    If an error occurs in this function it will be appended to the log and
    error mail messages, and the process status will be set appropriately.

    Parameters
    ----------
    ds_id : int
        Datastream ID.
    newfile - int
        Specifies if a new file should be created.

    Returns
    -------
    - Number of samples stored
    - 0 if no data found in dataset, or if all data samples were
      duplicates of previously stored data
    - -1 if an error occurred
    """
    return dsproc_store_dataset(ds_id, newfile)

def qc_limit_checks(cds3.core.Var var, cds3.core.Var qc_var, 
        int missing_flag, int min_flag, int max_flag):
    """Perform QC limit checks.

    This function will uses the following attributes to determine the missing_value,
    and min, max limits:

    - missing_value -r _FillValue, or default NetCDF fill value
    - valid_min
    - valid_max

    If a flag are set to zero that test is disabled

    If and error occurs in this function it will be appended to the log and error mail
    messages, and the process status will be set appropriately.   
    
    Parameters
    ----------
    var : cds3.core.Var
        Pointer to the variable
    qc_var : cds3.core.Var
        Pointer to the qc variable
    missing_flag : int
        QC flag to use for missing_values
    min_flag : int
        QC flag to use for values below the minimum
    max_flag : int
        QC flag to use for values above the maximum

    Returns
    -------
      - 1 if successful
      - 0 if an error occurred
    """
    return dsproc_qc_limit_checks(var.c_ob, qc_var.c_ob, missing_flag, min_flag, max_flag)

def get_bad_qc_mask(cds3.core.Var qc_var):
    """ Get the QC mask used to determine bad QC values.

    This function will use the bit assessment attributes to create a mask with all 
    bits set for bad assessment values. It will first check for field level bit 
    assessment attributes, and then for the global attributes if they are not found.

    The mask can be used to test to see if a bad bit is set by applying a bit-wise
    and operation.  If that returns zero then a bit with a bad assessment is set.
    
    Parameters
    ----------
    qc_var : cds3.core.Var
        Pointer to the QC variable
    
    Returns
    -------
    qc_mask - the QC mask with all bad bits set.
    """

    return dsproc_get_bad_qc_mask(qc_var.c_ob)


#CDSAtt *dsproc_get_dsdod_att(
#            int         ds_id,
#            char *var_name,
#            char *att_name)
#
#CDSDim *dsproc_get_dsdod_dim(
#            int         ds_id,
#            char *dim_name)
#
#void   *dsproc_get_dsdod_att_value(
#            int          ds_id,
#            char  *var_name,
#            char  *att_name,
#            CDSDataType  type,
#            size_t      *length,
#            void        *value)
#
#char   *dsproc_get_dsdod_att_text(
#            int         ds_id,
#            char *var_name,
#            char *att_name,
#            size_t     *length,
#            char       *value)
#
#size_t  dsproc_get_dsdod_dim_length(
#            int         ds_id,
#            char *dim_name)
#int     dsproc_set_dsdod_att_value(
#            int          ds_id,
#            char  *var_name,
#            char  *att_name,
#            CDSDataType  type,
#            size_t       length,
#            void        *value)
#
#int     dsproc_set_dsdod_att_text(
#            int         ds_id,
#            char *var_name,
#            char *att_name,
#            char *format, ...)
#
#int     dsproc_set_dsdod_dim_length(
#            int         ds_id,
#            char *dim_name,
#            size_t      dim_length)
#
def set_input_source(object status):
    """Set the input source string to use when new datasets are created.
       This function will set the string to use for the input_source 
       global attribute value when new datasets are created. This value 
       will only be set in datasets that have the input_source attribute 
       defined.
    """
    cdef object b_status = _to_byte_c_string( status )
    dsproc_set_status(b_status)

#int     dsproc_set_runtime_metadata(int ds_id, CDSGroup *cds)
#
#int     dsproc_update_datastream_dsdods(time_t data_time)
#
#int     dsproc_db_connect()

def db_disconnect():
    dsproc_db_disconnect()

#int     dsproc_get_input_ds_classes(DSClass ***ds_classes)
#int     dsproc_get_output_ds_classes(DSClass ***ds_classes)
#
def get_location():
    """
    Get process location.
    The memory used by output location structure belongs to the internal 
    structures and must not be freed or modified by the calling process.    

    Parameters
    ----------
    location :  output
        Pointer to the Proc_Loc structure.
        ProcLoc members (name, lat, lon, and alt) are returned as properties
    
    Returns
    -------
      - location structure if found
      - 0 if no location found
      - -1 if an error occured
    """
    cdef ProcLoc *proc_loc
    cdef int status
    cdef PyProcLoc location
    status = dsproc_get_location(&proc_loc)
    location = PyProcLoc()
    location.set_procloc(proc_loc)
    return location

#
#int     dsproc_get_config_value(
#            char  *config_key,
#            char       **config_value)
#
#int     dsproc_dqrdb_connect()
#void    dsproc_dqrdb_disconnect()
#
#void    dsproc_free_dqrs(DQR **dqrs)
#int     dsproc_get_dqrs(
#            char *site,
#            char *facility,
#            char *dsc_name,
#            char *dsc_level,
#            char *var_name,
#            time_t      start_time,
#            time_t      end_time,
#            DQR      ***dqrs)

def bad_file_warning(char *file_name, char *format, *args):
    s=format
    file,line,func=__line()
    if args:
        try:
            s = format % args
        except TypeError:
            s = format.format(*args)

    #dsproc_error(func, file, line, format, s)

    b_file_name = _to_byte_c_string( file_name )
    b_s  = _to_byte_c_string( s )
    dsproc_bad_file_warning(func, file, line, b_file_name, b_s )

#
#void dsproc_bad_line_warning(
#        char *sender,
#        char *func,
#        char *src_file,
#        int         src_line,
#        char *file_name,
#        int         line_num,
#        char *format, ...)
#
#void dsproc_bad_record_warning(
#        char *sender,
#        char *func,
#        char *src_file,
#        int         src_line,
#        char *file_name,
#        int         rec_num,
#        char *format, ...)
#
#int dsproc_load_transform_params_file(
#        CDSGroup   *group,
#        char *site,
#        char *facility,
#        char *name,
#        char *level)

def set_coordsys_trans_param(
        object coordsys_name,
        object field_name,
        object param_name,
        CDSDataType cds_type,
        object value):
    """Set the value of a coordinate system transformation parameter.

    Parameters
    ----------
    coordsys_name :  object
        The name of the coordinate system
    field_name : object
        Name of the field
    param_name : object
        Name of the transform parameter
    value : object
        The parameter value

    Returns
    -------
    - 1 if successful
    - 0 if the attribute does not exist
    """

    cdef np.ndarray value_nd
    cdef object byte_value
    cdef object b_coordsys_name = _to_byte_c_string( coordsys_name )
    cdef object b_field_name = _to_byte_c_string( field_name )
    cdef object b_param_name = _to_byte_c_string( param_name )
    cdef char c_value

    if cds_type == CDS_CHAR:
        byte_value = _to_byte_c_string(value)
        length = len(byte_value)
        if length == 1:
            value_nd = np.asarray(byte_value[0]) 
            return dsproc_set_coordsys_trans_param(b_coordsys_name, b_field_name,
              b_param_name, cds_type, length, value_nd.data)
        else:
            value_nd = np.asarray(byte_value)
            return dsproc_set_coordsys_trans_param(b_coordsys_name, b_field_name,
               b_param_name, cds_type, length, value_nd.data)

    value_nd = np.asarray(value, cds_type_to_dtype_obj(cds_type))
    if value_nd.ndim == 0:
        value_nd = value_nd[None] # add dummy dimension to a scalar value
    assert value_nd.ndim == 1
    length = len(value_nd)

    return dsproc_set_coordsys_trans_param(b_coordsys_name, b_field_name,
            b_param_name, cds_type, length, value_nd.data)

def delete_group(cds3.core.Group dataset):
    """Delete a dataset.
    
    Parameters
    ----------
    dataset : cds3.core.Group
        Pointer to the dataset
    
    Returns
    -------
    - 1 if successful
    - 0 if the group is locked or parent group is locked

    """
    return cds_delete_group(dataset.c_ob)

def _ingest_main_loop():
    cdef int       ndsid
    cdef int      *dsids
    cdef int       dsid
    cdef const char *level
    cdef int       nfiles
    cdef char    **files
    cdef const char *input_dir
    cdef time_t    loop_start
    cdef time_t    loop_end
    cdef time_t    time_remaining
    cdef int       status
    cdef int       dsi
    cdef int       fi
    
    cdef object    u_file
    cdef object    u_input_dir

    ndsids = dsproc_get_input_datastream_ids(&dsids)

    if ndsids == 0: 
        error("Could Not Find Input Datastream Class In Database\n","Could not find an input datastream defined in the database\n")
        return

    dsid = -1
    for dsi in range(ndsids):
        level = dsproc_datastream_class_level(dsids[dsi])
        if level[0] == '0':
            if dsid == -1: 
                dsid = dsids[dsi]             
            else:
                error("Could Not Find Input Datastream Class In Database\n",
                    "Too many level 0 input datastreams defined in database\n"
                    "  -> ingests only support one level 0 input datastream\n");
                return

    # Get the list of input files
    nfiles = dsproc_get_datastream_files(dsid, &files)
    if nfiles <= 0:
        if nfiles == 0:
            log("No data files found to process in: %s\n",
               _to_python_string( dsproc_datastream_path(dsid) ) )
            dsproc_set_status(DSPROC_ENODATA)
        return

    input_dir = dsproc_datastream_path(dsid)
    p_input_dir = _to_python_string( input_dir)

    # Loop over all input files
    loop_start = 0 
    loop_end = 0 
    for fi in range(nfiles):
        #Check the run time
        time_remaining = dsproc_get_time_remaining()
        if time_remaining >=0:
            if time_remaining == 0:
                break
            if loop_end - loop_start > time_remaining:
                log("\nStopping ingest before max run time of %d seconds is exceeded\n",
                   <int>dsproc_get_max_run_time())
                dsproc_set_status("Maximum Run Time Limit Exceeded")
                break 
    
        # Process the file
        # Python string type depends on version
        p_file = _to_python_string( files[fi] )

        val = fi+1
        debug_lv1("PROCESSING FILE #%d: %s\n", val, p_file )
        log("\nProcessing: %s/%s\n", p_input_dir, p_file )
        loop_start = time.time()
        dsproc_set_input_dir(input_dir)
        dsproc_set_input_source(files[fi])
        status = _run_process_file_hook(p_input_dir, p_file)
        if status == -1:
            break
        loop_end = time.time()

    dsproc_free_file_list(files)

def _vap_main_loop(int proc_model):
    cdef time_t    interval_begin
    cdef time_t    interval_end
    cdef int       status
    cdef cds3.core.Group ret_data = cds3.core.Group()
    cdef cds3.core.Group trans_data = cds3.core.Group()
    while dsproc_start_processing_loop(&interval_begin, &interval_end):
        QuicklookMode = dsproc_get_quicklook_mode()
        # Run the pre_retrieval_hook function
        status = _run_pre_retrieval_hook(interval_begin, interval_end)
        if status == -1: break
        if status ==  0: continue
     

        if QuicklookMode != QUICKLOOK_ONLY:
            # Retrieve the data for the current processing interval
            if proc_model & DSP_RETRIEVER:
                status = dsproc_retrieve_data(
                        interval_begin, interval_end, &ret_data.c_ob)
                if status == -1: break
                if status ==  0: continue
            # Run the post_retrieval_hook function
            status = _run_post_retrieval_hook(
                    interval_begin, interval_end, ret_data)
            if status == -1: break
            if status ==  0: continue
            # Merge the observations in the retrieved data
            if not dsproc_merge_retrieved_data(): break
            # Run the pre_transform_hook function
            status = _run_pre_transform_hook(
                    interval_begin, interval_end, ret_data)
            if status == -1: break
            if status ==  0: continue
            # Perform the data transformations for transform VAPs
            if proc_model & DSP_TRANSFORM:
                status = dsproc_transform_data(&trans_data.c_ob)
                if status == -1: break
                if status ==  0: continue
            # Run the post_transform_hook function
            status = _run_post_transform_hook(
                interval_begin, interval_end, trans_data)
            if status == -1: break
            if status ==  0: continue
            # Create output datasets
            if not dsproc_create_output_datasets(): break
            # Run the user's data processing function
            if trans_data:
                status = _run_process_data_hook(
                    interval_begin, interval_end, trans_data)
            else:
                status = _run_process_data_hook(
                    interval_begin, interval_end, ret_data)
            if status == -1: break
            if status ==  0: continue
            # Store all output datasets
            if not dsproc_store_output_datasets(): break

     
        if QuicklookMode != QUICKLOOK_DISABLE:
            # Run the quicklook_hook function
            status = _run_quicklook_hook(
                interval_begin, interval_end)
            if status == -1: break
            if status ==  0: continue
    

def main(argv, proc_model, proc_version, proc_names=None):
    """Datasystem Process Main Function.
    
    Parameters
    ----------
    argc         - command line argument count
    argv         - command line argument vector
    proc_model   - processing model to use
    proc_version - process version
    nproc_names  - number of valid process names
    proc_names   - list of valid process names
    
    Returns
    -------
      - suggested program exit value
        (0 = successful, 1 = failure)

    """

    cdef int exit_value
    #****************************************************************
    # Initialize the data system process.
    #
    # This function will not return if the -h (help) or -v (version)
    # option was specified on the command line, or if an error occurs
    # (i.e. could not connect to database, the process is not defined
    # in the database, the log file could not be opened/created, a
    # memory allocation error, etc...).
    #****************************************************************
    if proc_names is None:
        proc_names = []
    initialize(argv, proc_model, proc_version, proc_names)

    #****************************************************************
    # Call the user's init_process() function.
    #
    # The _run_init_process_hook() function will call the
    # user's init_process() function if one was set using the
    # dsproc_set_init_process_hook() function.
    #
    # When writing bindings for other languages the target language
    # must implement its own methods for setting and calling the
    # various user defined hook functions, and for storing a reference
    # to the user defined data structure.
    #****************************************************************
    if not _run_init_process_hook():
        exit_value = finish()
        sys.exit(exit_value)

    #****************************************************************
    # Disconnect from the database until it is needed again
    #****************************************************************
    dsproc_db_disconnect()

    #****************************************************************
    # Call the appropriate data processing loop
    #****************************************************************
    if proc_model == PM_INGEST:
        _ingest_main_loop() 
    else:
        _vap_main_loop(proc_model)

    #****************************************************************
    # Call the user's finish_process() function.
    #
    # The _run_finish_process_hook() function will call the
    # user's finish_process() function if one was set using the
    # dsproc_set_finish_process_hook() function.
    #
    # When writing bindings for other languages the target language
    # must implement its own methods for setting and calling the
    # various user defined hook functions, and for storing a reference
    # to the user defined data structure.
    #****************************************************************
    _run_finish_process_hook()

    #****************************************************************
    # Finish the data system process.
    #
    # This function will update the database and logs with process
    # status and metrics, close the logs, send any generated mail
    # messages, and cleanup all allocated memory.
    #****************************************************************
    exit_value = finish()

    return exit_value
