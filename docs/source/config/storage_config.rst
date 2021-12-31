.. _storage_config:

Storage Configuration
=====================
The storage config file (``storage_config.yml``) specifies configuration 
properties, the storage class will be used to save processed data, and 
declares file handlers that will be used to read/write data.

A simple annotated storage configuration is shown below:

.. code-block:: yaml

  ####################################################################
  # TSDAT INGEST PIPELINE STORAGE TEMPLATE
  ####################################################################
  storage:
    #-----------------------------------------------------------------
    # Storage Parameters
    #-----------------------------------------------------------------
    # This section should not be modified unless there is a strong need. 
    # Please contact the repository maintainers if you feel you need to 
    # use different settings here – there may be another way to accomplish 
    # what you need.
    
    classname: ${STORAGE_CLASSNAME}
    parameters:
      retain_input_files: ${RETAIN_INPUT_FILES}
      root_dir: ${ROOT_DIR}
      bucket_name: ${STORAGE_BUCKET}


    #-----------------------------------------------------------------
    # File Handler I/O
    #-----------------------------------------------------------------
    # This section specifies the file handlers to use to read the raw
    # datafile and to write the output file.
    
    file_handlers:
    
      # Input can be handled by tsdat's native file handlers or a
      # custom one, written by the user.
      input:
        custom:
          file_pattern: ".*.ext"
          classname: ingest.<ingest_name>.pipeline.filehandler.CustomFileHandler
        netcdf:
          file_pattern: ".*.nc"
          classname: tsdat.io.filehandlers.NetCdfHandler

      # Output is handled traditionally in tsdat using the netcdf4
      # format, but provides the option to output a csv. Please contact 
      # the repository maintainers if you feel you need to write to a 
      # different format.
      output:
        csv:
          file_extension: ".csv"
          classname: tsdat.io.filehandlers.CsvHandler
        netcdf:
          file_extension: ".nc"
          classname: tsdat.io.filehandlers.NetCdfHandler


File Handler Input/Output
^^^^^^^^^^^^^^^^^^^^^^^^^

Please see the file handler :ref:`configuration <filehandlers>` or :ref:`walkthrough <more_code>` 
for more detail.


Storage Parameters
^^^^^^^^^^^^^^^^^^

This section is primarily kept for reference and may not be of concern
to the majority of users. 
Currently there are two provided storage classes:

#. ``FilesystemStorage`` - saves to local filesystem
#. ``AwsStorage`` - saves to an AWS bucket (requires an AWS account with admin priviledges)

Each storage class has different configuration parameters, but they both share a common
file_handlers section. The following codeblocks explicitly state what each class's parameters
default to.

Local Filesystem:

.. code-block:: yaml

  storage: 
    classname:  tsdat.io.FilesystemStorage
    parameters:
      retain_input_files: True                 # Whether to keep input files after they are processed
      root_dir: ${CONFIG_DIR}/../storage/root  # The root dir where processed files will be stored


AWS S3 Bucket:

.. code-block:: yaml

  storage: 
    classname:  tsdat.io.AwsStorage
    parameters:
      retain_input_files: True                 # Whether to keep input files after they are processed
      bucket_name: tsdat_test                  # The name of the AWS S3 bucket where processed files will be stored
      root_dir: /storage/root                  # The root dir (key) prefix for all processed files created in the bucket
