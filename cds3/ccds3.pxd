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

from libc.stdio cimport FILE
from libc.stddef cimport size_t

cdef extern from "stdarg.h" nogil:
    ctypedef struct va_list:
        pass

cdef extern from "sys/types.h" nogil:
    ctypedef long time_t
    ctypedef long suseconds_t

cdef extern from "sys/time.h" nogil:
    cdef struct timeval:
        time_t tv_sec
        suseconds_t tv_usec

from ccds3_enums cimport *

cdef extern from "cds3.h" nogil:
    #
    # CDS User Data.
    #
    cdef struct CDSUserData:
        char  *key                 # user defined key
        void  *value               # user defined value
        void (*free_value)(void *) # function used to free the value

    #
    # CDS Object Definition.
    #
    #define _CDS_OBJECT_ \
    #    CDSObject     *parent    # parent object
    #    CDSObjectType  obj_type  # object type
    #    char          *obj_path  # object path
    #    int            def_lock  # definition lock
    #    CDSUserData  **user_data # user defined data
    #    char          *name      # object name

    #
    # CDS Object.
    #
    cdef struct CDSObject:
        CDSObject     *parent    # parent object
        CDSObjectType  obj_type  # object type
        char          *obj_path  # object path
        int            def_lock  # definition lock
        CDSUserData  **user_data # user defined data
        char          *name      # object name

    char   *cds_get_object_path(void *cds_object)
    void    cds_set_definition_lock(void *cds_object, int value)
    void    cds_delete_user_data(void *cds_object, char *key)
    void   *cds_get_user_data(void *cds_object, char *key)
    int     cds_set_user_data(
                void  *cds_object,
                char  *key,
                void  *value,
                void (*free_value)(void *))

    #
    # CDS Data Union.
    #
    ctypedef union CDSData:
        void        *vp #  void:   void pointer
        char        *cp #  char:   ISO/ASCII character
        signed char *bp #  byte:   signed 1 byte integer
        short       *sp #  short:  signed 2 byte integer
        int         *ip #  int:    signed 4 byte integer
        float       *fp #  float:  single precision floating point
        double      *dp #  double: double precision floating point

    CDSDataType cds_data_type(char *name)
    char       *cds_data_type_name(CDSDataType type)
    size_t      cds_data_type_size(CDSDataType type)
    void        cds_get_data_type_range(CDSDataType type, void *min, void *max)
    void        cds_get_default_fill_value(CDSDataType type, void *value)
    size_t      cds_max_type_size()

    #
    # CDS Group.
    #
    cdef struct CDSGroup:
        CDSObject     *parent           # parent object
        CDSObjectType  obj_type         # object type
        char          *obj_path         # object path
        int            def_lock         # definition lock
        CDSUserData  **user_data        # user defined data
        char          *name             # object name
        int            ndims            # number of dimensions
        CDSDim       **dims             # array of dimension pointers
        int            natts            # number of attributes
        CDSAtt       **atts             # array of attribute pointers
        int            nvars            # number of variables
        CDSVar       **vars             # array of variable pointers
        int            ngroups          # number of groups
        CDSGroup     **groups           # array of group pointers
        int            nvargroups       # number of variable groups
        CDSVarGroup  **vargroups        # array of variable group pointers
        void          *transform_params # transformation parameters

    CDSGroup   *cds_define_group(CDSGroup *parent, char *name)
    int         cds_delete_group(CDSGroup *group)
    CDSGroup   *cds_get_group   (CDSGroup *parent, char *name)
    int         cds_rename_group(CDSGroup *group, char *name)

    #
    # CDS Dimension.
    #
    cdef struct CDSDim:
        CDSObject     *parent       # parent object
        CDSObjectType  obj_type     # object type
        char          *obj_path     # object path
        int            def_lock     # definition lock
        CDSUserData  **user_data    # user defined data
        char          *name         # object name
        size_t         length       # dimension length
        int            is_unlimited # is unlimited flag (0 = FALSE, 1 = TRUE)

    int     cds_change_dim_length(CDSDim *dim, size_t length)
    CDSDim *cds_define_dim(CDSGroup   *group,
                char *name,
                size_t      length,
                int         is_unlimited)
    int     cds_delete_dim(CDSDim *dim)
    CDSDim *cds_get_dim(CDSGroup *group, char *name)
    CDSVar *cds_get_dim_var(CDSDim *dim)
    int     cds_rename_dim(CDSDim *dim, char *name)

    #
    # CDS Attribute.
    #
    cdef struct CDSAtt:
        CDSObject     *parent    # parent object
        CDSObjectType  obj_type  # object type
        char          *obj_path  # object path
        int            def_lock  # definition lock
        CDSUserData  **user_data # user defined data
        char          *name      # object name
        CDSDataType    type      # attribute data type
        size_t         length    # length of the attribute value
        CDSData        value     # attribute value

    CDSAtt *cds_change_att(
                void        *parent,
                int          overwrite,
                char        *name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    int     cds_change_att_value(
                CDSAtt      *att,
                CDSDataType  type,
                size_t       length,
                void        *value)
    int     cds_change_att_text(CDSAtt *att, char *format, ...)
    int     cds_change_att_va_list(CDSAtt *att, char *format, va_list args)
    CDSAtt *cds_define_att(
                void        *parent,
                char        *name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    CDSAtt *cds_define_att_text(void *parent, char *name, char *format, ...)
    CDSAtt *cds_define_att_va_list(
                void       *parent,
                char       *name,
                char       *format,
                va_list     args)
    int     cds_delete_att(CDSAtt *att)
    CDSAtt *cds_get_att(void *parent, char *name)
    void   *cds_get_att_value(
                CDSAtt       *att,
                CDSDataType   type,
                size_t       *length,
                void         *value)
    char   *cds_get_att_text(CDSAtt *att, size_t *length, char *value)
    int     cds_rename_att(CDSAtt *att, char *name)
    CDSAtt *cds_set_att(
                void        *parent,
                int          overwrite,
                char        *name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    int     cds_set_att_value(
                CDSAtt      *att,
                CDSDataType  type,
                size_t       length,
                void        *value)
    int     cds_set_att_text(CDSAtt *att, char *format, ...)
    int     cds_set_att_va_list(CDSAtt *att, char *format, va_list args)

    #
    # CDS Variable.
    #
    cdef struct CDSVar:
        CDSObject     *parent             # parent object
        CDSObjectType  obj_type           # object type
        char          *obj_path           # object path
        int            def_lock           # definition lock
        CDSUserData  **user_data          # user defined data
        char          *name               # object name
        CDSDataType    type               # data type
        int            ndims              # number of dimensions
        CDSDim       **dims               # array of dimension pointers
        int            natts              # number of attributes
        CDSAtt       **atts               # array of attribute pointers
        size_t         sample_count       # number of samples in the data array
        size_t         alloc_count        # number of samples allocated
        CDSData        data               # array of data values
        void          *data_index         # data index array
        int            data_index_ndims   # number of dims in data index array
        size_t        *data_index_lengths # dimension lengths of data index
        void          *default_fill       # default fill value

    CDSVar *cds_define_var(
                CDSGroup    *group,
                char        *name,
                CDSDataType  type,
                int          ndims,
                char       **dim_names)
    int     cds_delete_var(CDSVar *var)
    CDSVar *cds_get_coord_var(CDSVar *var, int dim_index)
    CDSVar *cds_get_var(CDSGroup *group, char *name)
    int     cds_rename_var(CDSVar *var, char *name)
    int     cds_var_is_unlimited(CDSVar *var)
    CDSDim *cds_var_has_dim(CDSVar *var, char *name)
    size_t  cds_var_sample_size(CDSVar *var)
    void   *cds_alloc_var_data(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count)
    void   *cds_alloc_var_data_index(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count)
    int     cds_change_var_type(CDSVar *var, CDSDataType type)
    int     cds_change_var_units(CDSVar *var, CDSDataType type, char *units)
    void   *cds_create_var_data_index(CDSVar *var)
    void    cds_delete_var_data(CDSVar *var)
    void   *cds_get_var_data(
                CDSVar       *var,
                CDSDataType   type,
                size_t        sample_start,
                size_t       *sample_count,
                void         *missing_value,
                void         *data)
    void   *cds_get_var_datap(CDSVar *var, size_t sample_start)
    int     cds_get_var_missing_values(CDSVar *var, void **values)
    char   *cds_get_var_units(CDSVar *var)
    void   *cds_init_var_data(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count,
                int     use_missing)
    void   *cds_init_var_data_index(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count,
                int     use_missing)
    void    cds_reset_sample_counts(
                CDSGroup *group,
                int       unlim_vars,
                int       static_vars)
    int     cds_set_var_default_fill_value(CDSVar *var, void *fill_value)
    void   *cds_set_var_data(
                CDSVar       *var,
                CDSDataType   type,
                size_t        sample_start,
                size_t        sample_count,
                void         *missing_value,
                void         *data)

    # Time Data
    ctypedef timeval timeval_t

    CDSVar *cds_find_time_var(void *object)
    time_t  cds_get_base_time(void *object)
    size_t  cds_get_time_range(
                void      *object,
                timeval_t *start_time,
                timeval_t *end_time)
    time_t *cds_get_sample_times(
                void   *object,
                size_t  sample_start,
                size_t *sample_count,
                time_t *sample_times)
    timeval_t *cds_get_sample_timevals(
                void      *object,
                size_t     sample_start,
                size_t    *sample_count,
                timeval_t *timevals)
    int     cds_set_base_time(void *object, char *long_name, time_t base_time)
    int     cds_set_sample_times(
                void   *object,
                size_t  sample_start,
                size_t  sample_count,
                time_t *times)
    int     cds_set_sample_timevals(
                void      *object,
                size_t     sample_start,
                size_t     sample_count,
                timeval_t *timevals)

    double TV_DOUBLE(timeval_t tv)
    bint TV_EQ(timeval_t tv1, timeval_t tv2)
    bint TV_NEQ(timeval_t tv1, timeval_t tv2)
    bint TV_GT(timeval_t tv1, timeval_t tv2)
    bint TV_GTEQ(timeval_t tv1, timeval_t tv2)
    bint TV_LT(timeval_t tv1, timeval_t tv2)
    bint TV_LTEQ(timeval_t tv1, timeval_t tv2)

    ## Cast timeval to double.
    #cdef inline double TV_DOUBLE(timeval_t tv):
    #    return ( (double)(tv).tv_sec + (1E-6 * (double)(tv).tv_usec) )

    ## Check if timeval 1 is equal to timeval 2.
    #cdef inline bint TV_EQ(timeval_t tv1, timeval_t tv2):
    #    return ( ( (tv1).tv_sec  == (tv2).tv_sec) and
    #             ( (tv1).tv_usec == (tv2).tv_usec) )

    ## Check if timeval 1 is not equal to timeval 2.
    #cdef inline bint TV_NEQ(timeval_t tv1, timeval_t tv2):
    #    return ( ( (tv1).tv_sec  != (tv2).tv_sec) or
    #             ( (tv1).tv_usec != (tv2).tv_usec) )

    ## Check if timeval 1 is greater than timeval 2.
    #cdef inline bint TV_GT(timeval_t tv1, timeval_t tv2):
    #    return ( ( (tv1).tv_sec == (tv2).tv_sec)  ?
    #             ( (tv1).tv_usec > (tv2).tv_usec) :
    #             ( (tv1).tv_sec  > (tv2).tv_sec) )

    ## Check if timeval 1 is greater than or equal to timeval 2.
    #cdef inline bint TV_GTEQ(tv1,tv2):
    #    return ( ( (tv1).tv_sec == (tv2).tv_sec)  ?
    #             ( (tv1).tv_usec >= (tv2).tv_usec) :
    #             ( (tv1).tv_sec  > (tv2).tv_sec) )

    ## Check if timeval 1 is less than timeval 2.
    #cdef inline bint TV_LT(tv1,tv2):
    #    return ( ( (tv1).tv_sec == (tv2).tv_sec)  ?
    #             ( (tv1).tv_usec < (tv2).tv_usec) :
    #             ( (tv1).tv_sec  < (tv2).tv_sec) )

    ## Check if timeval 1 is less than or equal to timeval 2.
    #cdef inline bint TV_LTEQ(tv1,tv2):
    #    return ( ( (tv1).tv_sec == (tv2).tv_sec)  ?
    #             ( (tv1).tv_usec <= (tv2).tv_usec) :
    #             ( (tv1).tv_sec  < (tv2).tv_sec) )

    int     cds_find_time_index(
                size_t   ntimes,
                time_t  *times,
                time_t   ref_time,
                int      mode)
    int     cds_find_timeval_index(
                size_t      ntimevals,
                timeval_t  *timevals,
                timeval_t   ref_timeval,
                int         mode)
    time_t  cds_get_midnight(time_t data_time)
    int     cds_is_time_var(CDSVar *var, int *is_base_time)
    int     cds_base_time_to_units_string(time_t base_time, char *units_string)
    int     cds_units_string_to_base_time(char *units_string, time_t *base_time)

    #
    # CDS Variable Group.
    #
    cdef struct CDSVarGroup:
        CDSObject     *parent    # parent object
        CDSObjectType  obj_type  # object type
        char          *obj_path  # object path
        int            def_lock  # definition lock
        CDSUserData  **user_data # user defined data
        char          *name      # object name
        int            narrays   # number of variable arrays in the group
        CDSVarArray  **arrays    # array of variable array pointers

    CDSVarArray *cds_add_vargroup_vars(
                    CDSVarGroup *vargroup,
                    char        *name,
                    int          nvars,
                    CDSVar     **vars)
    CDSVarGroup *cds_define_vargroup(CDSGroup *group, char *name)
    int          cds_delete_vargroup(CDSVarGroup *vargroup)
    CDSVarGroup *cds_get_vargroup(CDSGroup *group, char *name)

    #
    # CDS Variable Array.
    #
    cdef struct CDSVarArray:
        CDSObject     *parent    # parent object
        CDSObjectType  obj_type  # object type
        char          *obj_path  # object path
        int            def_lock  # definition lock
        CDSUserData  **user_data # user defined data
        char          *name      # object name
        int            nvars     # number of variables in the array
        CDSVar       **vars      # array of variable pointers

    int          cds_add_vararray_vars(
                    CDSVarArray *vararray,
                    int          nvars,
                    CDSVar     **vars)
    CDSVarArray *cds_create_vararray(
                    CDSGroup     *group,
                    char   *vargroup_name,
                    char   *vararray_name,
                    int           nvars,
                    char  **var_names)
    CDSVarArray *cds_define_vararray(CDSVarGroup *vargroup, char *name)
    int          cds_delete_vararray(CDSVarArray *vararray)
    CDSVarArray *cds_get_vararray(CDSVarGroup *vargroup, char *name)

    int     cds_print(FILE *fp, CDSGroup *group, int flags)
    int     cds_print_att(FILE *fp, char *indent, int  min_width, CDSAtt *att)
    int     cds_print_atts(FILE *fp, char *indent, void *parent)
    int     cds_print_dim(FILE *fp, char *indent, int min_width, CDSDim *dim)
    int     cds_print_dims(FILE *fp, char *indent, CDSGroup *group)
    int     cds_print_var(FILE *fp, char *indent, CDSVar *var, int flags)
    int     cds_print_vars(FILE *fp, char *indent, CDSGroup *group, int flags)
    int     cds_print_var_data(FILE *fp, char *label, char *indent, CDSVar *var)
    int     cds_print_data(FILE *fp, char *indent, CDSGroup *group)
    int     cds_print_group(FILE *fp, char *indent, CDSGroup *group, int flags)
    int     cds_print_groups(FILE *fp, char *indent, CDSGroup *group, int flags)
    int     cds_print_vararray(
                FILE        *fp,
                char        *indent,
                CDSVarArray *vararray,
                int          flags)
    int     cds_print_vargroup(
                FILE        *fp,
                char  *indent,
                CDSVarGroup *vargroup,
                int          flags)
    int     cds_print_vargroups(
                FILE       *fp,
                char *indent,
                CDSGroup   *group,
                int         flags)

    int     cds_set_transform_param(
                CDSGroup    *group,
                char  *obj_name,
                char  *param_name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    void   *cds_get_transform_param(
                void        *object,
                char  *param_name,
                CDSDataType  type,
                size_t      *length,
                void        *value)
    void   *cds_get_transform_param_from_group(
                CDSGroup    *group,
                char  *obj_name,
                char  *param_name,
                CDSDataType  type,
                size_t      *length,
                void        *value)
    int     cds_load_transform_params_file(
                CDSGroup   *group,
                char *path,
                char *file)
    int     cds_print_transform_params(
                FILE       *fp,
                char *indent,
                CDSGroup   *group,
                char *obj_name)

    #
    # CDS data converter.
    #
    ctypedef void * CDSConverter

    int     cds_add_data_att(char *name, int flags)
    void    cds_free_data_atts()
    int     cds_is_data_att(CDSAtt *att, int *flags)
    void   *cds_convert_array(
                CDSConverter  converter,
                int           flags,
                size_t        length,
                void         *in_data,
                void         *out_data)
    int     cds_convert_var(CDSConverter converter, CDSVar *var)
    CDSConverter cds_create_converter(
                CDSDataType  in_type,
                char  *in_units,
                CDSDataType  out_type,
                char  *out_units)
    CDSConverter cds_create_converter_array_to_var(
                CDSDataType  in_type,
                char  *in_units,
                size_t       in_nmissing,
                void        *in_missing,
                CDSVar      *out_var)
    CDSConverter cds_create_converter_var_to_array(
                CDSVar      *in_var,
                CDSDataType  out_type,
                char  *out_units,
                size_t       out_nmissing,
                void        *out_missing)
    CDSConverter cds_create_converter_var_to_var(
                CDSVar *in_var,
                CDSVar *out_var)
    void    cds_destroy_converter(CDSConverter converter)
    int     cds_set_converter_map(
                CDSConverter converter,
                size_t       in_map_length,
                void        *in_map,
                size_t       out_map_length,
                void        *out_map)
    int     cds_set_converter_range(
                CDSConverter  converter,
                void         *out_min,
                void         *orv_min,
                void         *out_max,
                void         *orv_max)

    int cds_copy_att(
            CDSAtt      *src_att,
            void        *dest_parent,
            char  *dest_name,
            int          flags,
            CDSAtt     **dest_att)
    int cds_copy_atts(
            void        *src_parent,
            void        *dest_parent,
            char **src_names,
            char **dest_names,
            int          flags)
    int cds_copy_dim(
            CDSDim     *src_dim,
            CDSGroup   *dest_group,
            char *dest_name,
            int         flags,
            CDSDim    **dest_dim)
    int cds_copy_dims(
            CDSGroup    *src_group,
            CDSGroup    *dest_group,
            char **src_names,
            char **dest_names,
            int          flags)
    int cds_copy_var(
            CDSVar      *src_var,
            CDSGroup    *dest_group,
            char  *dest_name,
            char **src_dim_names,
            char **dest_dim_names,
            char **src_att_names,
            char **dest_att_names,
            size_t       src_start,
            size_t       dest_start,
            size_t       sample_count,
            int          flags,
            CDSVar     **dest_var)
    int cds_copy_vars(
            CDSGroup    *src_group,
            CDSGroup    *dest_group,
            char **src_dim_names,
            char **dest_dim_names,
            char **src_var_names,
            char **dest_var_names,
            size_t       src_start,
            size_t       dest_start,
            size_t       sample_count,
            int          flags)
    int cds_copy_group(
            CDSGroup    *src_group,
            CDSGroup    *dest_parent,
            char  *dest_name,
            char **src_dim_names,
            char **dest_dim_names,
            char **src_att_names,
            char **dest_att_names,
            char **src_var_names,
            char **dest_var_names,
            char **src_subgroup_names,
            char **dest_subgroup_names,
            size_t       src_start,
            size_t       dest_start,
            size_t       sample_count,
            int          flags,
            CDSGroup   **dest_group)
    int cds_copy_subgroups(
            CDSGroup    *src_group,
            CDSGroup    *dest_group,
            char **src_dim_names,
            char **dest_dim_names,
            char **src_att_names,
            char **dest_att_names,
            char **src_var_names,
            char **dest_var_names,
            char **src_subgroup_names,
            char **dest_subgroup_names,
            size_t       src_start,
            size_t       dest_start,
            size_t       sample_count,
            int          flags)


    #
    # generic unit converter type.
    #
    ctypedef void * CDSUnitConverter

    int     cds_compare_units(char *from_units, char *to_units)
    void    *cds_convert_units(
                CDSUnitConverter  converter,
                CDSDataType       in_type,
                size_t            length,
                void             *in_data,
                CDSDataType       out_type,
                void             *out_data,
                size_t            nmap,
                void             *in_map,
                void             *out_map,
                void             *out_min,
                void             *orv_min,
                void             *out_max,
                void             *orv_max)
    void    *cds_convert_unit_deltas(
                CDSUnitConverter  converter,
                CDSDataType       in_type,
                size_t            length,
                void             *in_data,
                CDSDataType       out_type,
                void             *out_data,
                size_t            nmap,
                void             *in_map,
                void             *out_map)
    void    cds_free_unit_converter(CDSUnitConverter converter)
    void    cds_free_unit_system()
    int     cds_get_unit_converter(
                char       *from_units,
                char       *to_units,
                CDSUnitConverter *converter)
    int     cds_init_unit_system(char *xml_db_path)
    int     cds_map_symbol_to_unit(char *symbol, char *unit_name)

    # Core Utility Functions
    int     cds_compare_arrays(
                size_t       length,
                CDSDataType  array1_type,
                void        *array1,
                CDSDataType  array2_type,
                void        *array2,
                void        *threshold,
                size_t      *diff_index)
    void    *cds_copy_array(
                CDSDataType  in_type,
                size_t       length,
                void        *in_data,
                CDSDataType  out_type,
                void        *out_data,
                size_t       nmap,
                void        *in_map,
                void        *out_map,
                void        *out_min,
                void        *orv_min,
                void        *out_max,
                void        *orv_max)
    void   *cds_create_data_index(
                void        *data,
                CDSDataType  type,
                int          ndims,
                size_t      *lengths)
    void    cds_free_data_index(
                void   *index,
                int     ndims,
                size_t *lengths)
    void   *cds_get_missing_values_map(
                CDSDataType  in_type,
                int          nmissing,
                void        *in_missing,
                CDSDataType  out_type,
                void        *out_missing)
    void   *cds_init_array(
                CDSDataType  type,
                size_t       length,
                void        *fill_value,
                void        *array)
    void   *cds_memdup(size_t nbytes, void *memp)
    size_t  cds_print_array(
                FILE        *fp,
                CDSDataType  type,
                size_t       length,
                void        *array,
                char  *indent,
                size_t       maxline,
                size_t       linepos,
                int          flags)
    char *cds_sprint_array(
                CDSDataType  type,
                size_t       array_length,
                void        *array,
                size_t      *string_length,
                char        *string,
                char  *indent,
                size_t       maxline,
                size_t       linepos,
                int          flags)
    void   *cds_string_to_array(
                char  *string,
                CDSDataType  type,
                size_t      *length,
                void        *array)
    char   *cds_array_to_string(
                CDSDataType  type,
                size_t       array_length,
                void        *array,
                size_t      *string_length,
                char        *string)

    # Library Version
    char *cds_lib_version()

#******************************************************************************
# DEPRECATED
#******************************************************************************

    void *cds_put_var_data(
            CDSVar      *var,
            size_t       sample_start,
            size_t       sample_count,
            CDSDataType  type,
            void        *data)

