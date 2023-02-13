#*******************************************************************************
#
#  COPYRIGHT (C) 2012 Battelle Memorial Institute.  All Rights Reserved.
#
#*******************************************************************************
#
#  Author:
#     name:  Jeff Daily
#     phone: (509) 372-6548
#     email: jeff.daily@pnnl.gov
#
#*******************************************************************************

from libc.stdio cimport FILE
from cds3.ccds3 cimport *

cdef extern from "dsdb3.h" nogil:
    ctypedef struct DSClass:
        char *name
        char *level
    struct DQR:
        pass
    ctypedef struct ProcLoc:
        char  *name
        float  lat
        float  lon
        float  alt

cdef extern from "sys/types.h" nogil:
    ctypedef int time_t 

from cdsproc3_enums cimport *

cdef extern from "dsproc3.h" nogil:
    int dsproc_get_debug_level()
    
    char *dsproc_get_site()
    char *dsproc_get_facility()
    char *dsproc_get_name()
    
    int     dsproc_get_datastream_id(
                char *site,
                char *facility,
                char *dsc_name,
                char *dsc_level,
                DSRole      role)
    
    int     dsproc_get_input_datastream_id(
                char *dsc_name,
                char *dsc_level)
    
    int     dsproc_get_input_datastream_ids(int **ids)
    
    int     dsproc_get_output_datastream_id(
                char *dsc_name,
                char *dsc_level)
    
    int     dsproc_get_output_datastream_ids(int **ids)
    
    char *dsproc_datastream_name(int ds_id)
    char *dsproc_datastream_site(int ds_id)
    char *dsproc_datastream_facility(int ds_id)
    char *dsproc_datastream_class_name(int ds_id)
    char *dsproc_datastream_class_level(int ds_id)
    char *dsproc_datastream_path(int ds_id)

    int  dsproc_getopt(
               const char *option, 
               const char **value) 
      
    int  dsproc_setopt(
               const char short_opt, 
               const char *long_opt, 
               const char *arg_name, 
               const char *opt_desc) 

    void dsproc_use_nc_extension()

    void dsproc_disable_lock_file()
    
    char *dsproc_dataset_name(CDSGroup *dataset)
    
    CDSGroup   *dsproc_get_output_dataset(
                   int ds_id,
                   int obs_index)
    
    CDSGroup   *dsproc_get_retrieved_dataset(
                   int ds_id,
                   int obs_index)
    
    CDSGroup   *dsproc_get_transformed_dataset(
                   char *coordsys_name,
                   int         ds_id,
                   int         obs_index)
    
    CDSDim *dsproc_get_dim(
                CDSGroup   *dataset,
                char *name)
    
    size_t  dsproc_get_dim_length(
                CDSGroup   *dataset,
                char *name)
    
    int     dsproc_set_dim_length(
                CDSGroup    *dataset,
                char  *name,
                size_t       length)
    
    int     dsproc_change_att(
                void        *parent,
                int          overwrite,
                char  *name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    
    CDSAtt *dsproc_get_att(
                void        *parent,
                char  *name)
    
    char   *dsproc_get_att_text(
                void       *parent,
                char *name,
                size_t     *length,
                char       *value)
    
    void   *dsproc_get_att_value(
                void        *parent,
                char  *name,
                CDSDataType  type,
                size_t      *length,
                void        *value)
    
    int     dsproc_set_att(
                void        *parent,
                int          overwrite,
                char  *name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    
    int     dsproc_set_att_text(
                void        *parent,
                char  *name,
                char *format, ...)
    
    int     dsproc_set_att_value(
                void        *parent,
                char  *name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    
    CDSVar *dsproc_clone_var(
                CDSVar       *src_var,
                CDSGroup     *dataset,
                char   *var_name,
                CDSDataType   data_type,
                char  **dim_names,
                int           copy_data)
    
    CDSVar *dsproc_define_var(
                CDSGroup    *dataset,
                char  *name,
                CDSDataType  type,
                int          ndims,
                char **dim_names,
                char  *long_name,
                char  *standard_name,
                char  *units,
                void        *valid_min,
                void        *valid_max,
                void        *missing_value,
                void        *fill_value)
    
    int     dsproc_delete_var(
                CDSVar *var)
    
    CDSVar *dsproc_get_coord_var(
                CDSVar *var,
                int     dim_index)
    
    int     dsproc_get_dataset_vars(
                CDSGroup     *dataset,
                char  **var_names,
                int           required,
                CDSVar     ***vars,
                CDSVar     ***qc_vars,
                CDSVar     ***aqc_vars)
    
    CDSVar *dsproc_get_metric_var(
                CDSVar  *var,
                char *metric)

    CDSVar *dsproc_get_output_var(
                int         ds_id,
                char *var_name,
                int         obs_index)
    
    CDSVar *dsproc_get_qc_var(
                CDSVar *var)
    
    CDSVar *dsproc_get_retrieved_var(
                char *var_name,
                int         obs_index)
    
    CDSVar *dsproc_get_transformed_var(
                char *var_name,
                int         obs_index)
    
    CDSVar *dsproc_get_trans_coordsys_var(
                char *coordsys_name,
                char *var_name,
                int         obs_index)
    
    CDSVar *dsproc_get_var(
                CDSGroup   *dataset,
                char *name)
    
    char *dsproc_var_name(CDSVar *var)
    size_t      dsproc_var_sample_count(CDSVar *var)
    size_t      dsproc_var_sample_size(CDSVar *var)
    
    void   *dsproc_alloc_var_data(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count)
    
    void   *dsproc_alloc_var_data_index(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count)
    
    void   *dsproc_get_var_data_index(
                CDSVar *var)
    
    void   *dsproc_get_var_data(
                CDSVar       *var,
                CDSDataType   type,
                size_t        sample_start,
                size_t       *sample_count,
                void         *missing_value,
                void         *data)
    
    int     dsproc_get_var_missing_values(
                CDSVar  *var,
                void   **values)
    
    void   *dsproc_init_var_data(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count,
                int     use_missing)
    
    void   *dsproc_init_var_data_index(
                CDSVar *var,
                size_t  sample_start,
                size_t  sample_count,
                int     use_missing)
    
    void   *dsproc_set_var_data(
                CDSVar       *var,
                CDSDataType   type,
                size_t        sample_start,
                size_t        sample_count,
                void         *missing_value,
                void         *data)
    
    time_t  dsproc_get_base_time(void *cds_object)
    
    size_t  dsproc_get_time_range(
                void      *cds_object,
                timeval_t *start_time,
                timeval_t *end_time)
    
    CDSVar *dsproc_get_time_var(void *cds_object)
    
    time_t *dsproc_get_sample_times(
                void   *cds_object,
                size_t  sample_start,
                size_t *sample_count,
                time_t *sample_times)
    
    timeval_t *dsproc_get_sample_timevals(
                void      *cds_object,
                size_t     sample_start,
                size_t    *sample_count,
                timeval_t *sample_times)

    timeval_t *dsproc_fetch_timevals(
                int       ds_id,
                timeval_t *begin_time,
                timeval_t *end_time,
                size_t    *ntimevals,
                timeval_t *timevals)

    int     dsproc_set_base_time(
                void      *cds_object,
                char      *long_name,
                time_t    base_time)
    
    int     dsproc_set_sample_times(
                void     *dataset,
                size_t    sample_start,
                size_t    sample_count,
                time_t   *sample_times)
    
    int     dsproc_set_sample_timevals(
                void      *dataset,
                size_t     sample_start,
                size_t     sample_count,
                timeval_t *sample_times)

    ctypedef struct VarTarget:
        int         ds_id
        char *var_name

    int     dsproc_add_var_output_target(
                CDSVar     *var,
                int         ds_id,
                char *var_name)
    
    int     dsproc_copy_var_tag(
                CDSVar *src_var,
                CDSVar *dest_var)
    
    void    dsproc_delete_var_tag(
                CDSVar *var)

    int     dsproc_get_source_ds_id(
                CDSVar *var)

    char    *dsproc_get_source_var_name(
                CDSVar *var)

    char    *dsproc_get_source_ds_name(
                CDSVar *var)
    
    int     dsproc_get_var_output_targets(
                CDSVar      *var,
                VarTarget ***targets)
    
    int     dsproc_set_var_coordsys_name(
                CDSVar     *var,
                char *coordsys_name)
    
    int     dsproc_set_var_flags(CDSVar *var, int flags)
    
    int     dsproc_set_var_output_target(
                CDSVar     *var,
                int         ds_id,
                char *var_name)
    
    void    dsproc_unset_var_flags(CDSVar *var, int flags)

    ctypedef struct VarDQR:
        char  *id          # DQR ID
        char  *desc        # description
        char  *ds_name     # datastream name
        char  *var_name    # variable name
        int          code        # code number
        char  *color       # code color
        char  *code_desc   # code description
        time_t       start_time  # start time in seconds since 1970
        time_t       end_time    # end time in seconds since 1970
        size_t       start_index # start time index in dataset
        size_t       end_index   # end time index in dataset

    int dsproc_get_var_dqrs(CDSVar *var, VarDQR ***dqrs)
    
    int dsproc_dump_dataset(
            CDSGroup   *dataset,
            char *outdir,
            char *prefix,
            time_t      file_time,
            char *suffix,
            int         flags)
    
    int dsproc_dump_output_datasets(
            char *outdir,
            char *suffix,
            int         flags)
    
    int dsproc_dump_retrieved_datasets(
            char *outdir,
            char *suffix,
            int         flags)
    
    int dsproc_dump_transformed_datasets(
            char *outdir,
            char *suffix,
            int         flags)
    
    int     dsproc_copy_file(char *src_file, char *dest_file)
    int     dsproc_move_file(char *src_file, char *dest_file)
    FILE   *dsproc_open_file(char *file)

    int dsproc_create_timestamp(
            time_t    secs1970,
            char      *timestamp)

    int dsproc_execvp(
            char *file,
            char **inargv,
            int dsc_level)

    int     dsproc_run_dq_inspector(
                int          ds_id,
                time_t      begin_time,
                time_t      end_time,
                char      **input_args,
                int         flag)

    int     dsproc_find_datastream_files(
                int          ds_id,
                time_t      begin_time,
                time_t      end_time,
                char ***file_list)
    
    int     dsproc_add_datastream_file_patterns(
                int          ds_id,
                int          npatterns,
                char **patterns,
                int          ignore_case)

    int     dsproc_set_file_name_time_patterns(
                int          ds_id,
                int          npatterns,
                char **patterns)
    
    void    dsproc_set_datastream_file_extension(
                int   ds_id,
                char *extension)
 
    void    dsproc_free_file_list(
                char **file_list)
    
    int     dsproc_get_datastream_files(
                int     ds_id,
                char ***file_list)

    void    dsproc_set_datastream_split_mode(
                int       ds_id,
                SplitMode split_mode,
                double    split_start,
                double    split_interval)
    
# KLG waiting for input from Brian on whether I need this
#    void    dsproc_set_file_name_compare_function(
#                int     ds_id,
#                int    (*function)(void *, void *))

    int     dsproc_rename(
                int         ds_id,
                char *file_path,
                char *file_name,
                time_t      begin_time,
                time_t      end_time)
    
    int     dsproc_rename_tv(
                int              ds_id,
                char      *file_path,
                char      *file_name,
                timeval_t *begin_time,
                timeval_t *end_time)
    
    int     dsproc_rename_bad(
                int         ds_id,
                char *file_path,
                char *file_name,
                time_t      file_time)
    
    void    dsproc_set_rename_preserve_dots(int ds_id, int preserve_dots)

cdef extern from "dsproc3_internal.h" nogil:

    void dsproc_initialize(
            int          argc,
            char       **argv,
            ProcModel    proc_model,
            char  *proc_version,
            int          nproc_names,
            char **proc_names)
    
    int dsproc_start_processing_loop(
            time_t *interval_begin,
            time_t *interval_end)
    
    int dsproc_retrieve_data(
            time_t     begin_time,
            time_t     end_time,
            CDSGroup **ret_data)
    
    int dsproc_get_quicklook_mode()

    int dsproc_merge_retrieved_data()
    
    int dsproc_transform_data(
            CDSGroup **trans_data)
    
    int dsproc_create_output_datasets()
    
    int dsproc_store_output_datasets()
    
    int dsproc_finish()

    void dsproc_debug(
            char *func,
            char *file,
            int         line,
            int         level,
            char *format, ...)
    
    void dsproc_error(
            char *func,
            char *file,
            int         line,
            char *status,
            char *format, ...)
    
    void dsproc_abort(
            char *func,
            char *file,
            int         line,
            char *status,
            char *format, ...)
    
    void dsproc_log(
            char *func,
            char *file,
            int         line,
            char *format, ...)
    
    void dsproc_mentor_mail(
            char *func,
            char *file,
            int         line,
            char *format, ...)
    
    void dsproc_warning(
            char *func,
            char *file,
            int         line,
            char *format, ...)
    
    void dsproc_disable(char *message)
    void dsproc_disable_db_updates()
    void dsproc_disable_lock_file()
    void dsproc_disable_mail_messages()

    void dsproc_set_processing_interval(
                time_t begin_time, 
                time_t end_time)
    void dsproc_set_processing_interval_offset(time_t offset)
    void dsproc_set_datastream_split_tz_offset(int ds_id, int split_tz_offset)
    void dsproc_set_retriever_time_offsets(
                int ds_id,
                time_t begin_offset,
                time_t end_offset)
    void dsproc_set_trans_qc_rollup_flag(int flag)
    
    char *dsproc_lib_version()
    
    char *dsproc_get_status()
    char *dsproc_get_type()
    char *dsproc_get_version()
    int dsproc_get_force_mode()

    unsigned int dsproc_get_missing_value_bit_flag(int bit_ndescs, const char **bit_descs)
    int dsproc_get_qc_bit_descriptions(CDSVar *qc_var, const char ***bit_descs)
    
    time_t      dsproc_get_max_run_time()
    time_t      dsproc_get_start_time()
    time_t      dsproc_get_time_remaining()
    time_t      dsproc_get_min_valid_time()
    time_t      dsproc_get_data_interval()
    time_t      dsproc_get_processing_interval(time_t *begin, time_t *end)
    
    void        dsproc_set_status(char *status)

    int     dsproc_init_datastream(
                char  *site,
                char  *facility,
                char  *dsc_name,
                char  *dsc_level,
                DSRole       role,
                char  *path,
                DSFormat     format,
                int          flags)
    
    void    dsproc_update_datastream_data_stats(
                int              ds_id,
                int              num_records,
                timeval_t *begin_time,
                timeval_t *end_time)
    
    void    dsproc_update_datastream_file_stats(
                int              ds_id,
                double           file_size,
                timeval_t *begin_time,
                timeval_t *end_time)
    
    int     dsproc_validate_datastream_data_time(
                int              ds_id,
                timeval_t *data_time)
    
    void    dsproc_set_datastream_flags(int ds_id, int flags)
    void    dsproc_set_datastream_format(int ds_id, DSFormat format)
    int     dsproc_set_datastream_path(int ds_id, char *path)
    
    void    dsproc_unset_datastream_flags(int ds_id, int flags)
    
    CDSGroup *dsproc_create_output_dataset(
                int      ds_id,
                time_t   data_time,
                int      set_location)
    
    int     dsproc_dataset_pass_through(
                CDSGroup *in_cds,
                CDSGroup *out_cds,
                int       flags)
    
    int     dsproc_map_datasets(
                CDSGroup *in_parent,
                CDSGroup *out_dataset,
                int       flags)

    void    dsproc_set_map_time_range(
                time_t      begin_time,
                time_t      end_time)
    
    int     dsproc_map_var(
                CDSVar *in_var,
                size_t  in_sample_start,
                size_t  sample_count,
                CDSVar *out_var,
                size_t  out_sample_start,
                int     flags)
    
    int     dsproc_set_dataset_location(CDSGroup *cds)
    
    int     dsproc_store_dataset(
                int ds_id,
                int newfile)

    int     dsproc_qc_limit_checks(
                CDSVar *var,
                CDSVar *qc_var,
                int missing_flag,
                int min_flag,
                int max_flag)

    unsigned int  dsproc_get_bad_qc_mask(
                  CDSVar *var)

    CDSAtt *dsproc_get_dsdod_att(
                int         ds_id,
                char *var_name,
                char *att_name)
    
    CDSDim *dsproc_get_dsdod_dim(
                int         ds_id,
                char *dim_name)
    
    void   *dsproc_get_dsdod_att_value(
                int          ds_id,
                char  *var_name,
                char  *att_name,
                CDSDataType  type,
                size_t      *length,
                void        *value)
    
    char   *dsproc_get_dsdod_att_text(
                int         ds_id,
                char *var_name,
                char *att_name,
                size_t     *length,
                char       *value)
    
    size_t  dsproc_get_dsdod_dim_length(
                int         ds_id,
                char *dim_name)
    
    int     dsproc_set_dsdod_att_value(
                int          ds_id,
                char  *var_name,
                char  *att_name,
                CDSDataType  type,
                size_t       length,
                void        *value)
    
    int     dsproc_set_dsdod_att_text(
                int         ds_id,
                char *var_name,
                char *att_name,
                char *format, ...)
    
    int     dsproc_set_dsdod_dim_length(
                int         ds_id,
                char *dim_name,
                size_t      dim_length)
    
    void    dsproc_set_input_dir(const char *input_dir)
    void    dsproc_set_input_source(char *input_source)
    int     dsproc_set_runtime_metadata(int ds_id, CDSGroup *cds)
    
    int     dsproc_update_datastream_dsdods(time_t data_time)
    
    int     dsproc_db_connect()
    void    dsproc_db_disconnect()
    
    int     dsproc_get_input_ds_classes(DSClass ***ds_classes)
    int     dsproc_get_output_ds_classes(DSClass ***ds_classes)
    
    int     dsproc_get_location(ProcLoc **proc_loc)
    
    int     dsproc_get_config_value(
                char  *config_key,
                char       **config_value)
    
    int     dsproc_dqrdb_connect()
    void    dsproc_dqrdb_disconnect()
    
    void    dsproc_free_dqrs(DQR **dqrs)
    int     dsproc_get_dqrs(
                char *site,
                char *facility,
                char *dsc_name,
                char *dsc_level,
                char *var_name,
                time_t      start_time,
                time_t      end_time,
                DQR      ***dqrs)
    
    void dsproc_bad_file_warning(
            char *func,
            char *src_file,
            int         src_line,
            char *file_name,
            char *format, ...)

    void dsproc_bad_line_warning(
            char *func,
            char *src_file,
            int         src_line,
            char *file_name,
            int         line_num,
            char *format, ...)
    
    void dsproc_bad_record_warning(
            char *func,
            char *src_file,
            int         src_line,
            char *file_name,
            int         rec_num,
            char *format, ...)
    
    int dsproc_load_transform_params_file(
            CDSGroup   *group,
            char *site,
            char *facility,
            char *name,
            char *level)

    int     dsproc_set_coordsys_trans_param(
                char        *coordsys_name,
                char        *field_name,
                char        *param_name,
                CDSDataType type,
                size_t      length,
                void        *value)

    int dsproc_fetch_dataset(
            int       ds_id,
            timeval_t *begin_timeval,
            timeval_t *end_timeval,
            size_t    nvars,
            char      **var_names,
            int       merge_obs,
            CDSGroup  **dataset)
