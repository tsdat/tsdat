
.. _configuring_tsdat:

#################
Configuring Tsdat
#################

Tsdat pipelines can be configured to tailor the specific data and metadata that will be contained
in the standardized dataset.  Tsdat pipelines provide multiple layers of configuration to allow the
community to easily contribute common functionality (such as unit converters or file readers), to 
provide a low intial barrier of entry for basic ingests, and to allow
full customization of the pipeline for very unique circumstances.  The following figure illustrates the
different phases of the pipeline along with multiple layers of configuration that Tsdat provides.

.. figure:: figures/configuration.png
   :alt: Tsdat pipelines provide multiple levels of configuration.

As shown in the figure, users can customize Tsdat in two ways:

#. **Configuration files** - shown as input to the pipeline on the left
#. **Code hooks** - indicated in the pipeline diagram with code (<>) bubbles.  Code hooks are provided by
   extending a specific base class as explained below.  Code hooks with a GREEN outline are methods contained within
   the custom IngestPipeline class that runs the pipeline.  Code hooks with a PURPLE outline
   are classes created outside the pipeline, but are declared in the storage config file.  Code hooks
   with a RED outline are classes created outside the pipeline, but are declared in the pipeline config file.


More information on config file syntax and code hook base classes are provided below.

.. note::
   Tsdat pipelines produce standardized datasets that follow the conventions and terminology provided 
   in the `Data Standards Document <https://github.com/ME-Data-Pipeline-Software/data_standards>`_.
   Please refer to this document for more detailed information about the format of standardized datasets.

********************
Configuration Files
********************
Configuration files provide an explict, declarative way to define and customize the behavior
of tsdat data pipelines. There are two types of configuration files:

#. **Storage config**
#. **Pipeline config**

This section
breaks down the various properties of both types of configuration files and 
shows how these files can be modified to support a wide variety of data 
pipelines.

.. note::
   Config files are written in yaml format.  We recommend using an IDE with
   yaml support (such as VSCode) for editing your config files.

.. note::
   In addition to your pre-configured pipeline template, see the `tsdat examples <https://github.com/tsdat/tsdat/tree/master/examples>`_
   folder for more configuration examples.

.. note::
   In your pipeline template project, configuration files can be found in the config/ folder.

.. _storage_config:

=============================
Storage Config
=============================
The storage config file specifies which Storage class will be used to save processed
data, declares configuration properties for that Storage (such as the root folder), and
declares various FileHandler classses that will be used to read/write data with the
specified file extensions.

Currently there are two provided storage classes:

#. **FilesystemStorage** - saves to local filesystem
#. **AwsStorage** - saves to an AWS bucket (requires an AWS account with admin priviledges)

Each storage class has different configuration parameters, but they both share a common
file_handlers section as explained below.

.. note::
   Environment variables can be referenced in the storage config file using **${PARAMETER}**
   syntax in the yaml.  Any referenced environment variables need to be set via the shell or via
   the  ``os.environ`` dictionary from your run_pipeline.py file.  
   The CONFIG_DIR environment parameter set automatically by tsdat and refers to the folder where
   the storage config file is located.
   

-----------------------------
FilesystemStorage Parameters
-----------------------------
.. code-block:: yaml

	storage: 
		classname:  tsdat.io.FilesystemStorage       # Choose from FilesystemStorage or AwsStorage
		parameters:
			retain_input_files: True                 # Whether to keep input files after they are processed
			root_dir: ${CONFIG_DIR}/../storage/root  # The root dir where processed files will be stored

-----------------------------
AwsStorage Parameters
-----------------------------
.. code-block:: yaml

	storage: 
		classname:  tsdat.io.AwsStorage              # Choose from FilesystemStorage or AwsStorage
		parameters:
			retain_input_files: True                 # Whether to keep input files after they are processed
			bucket_name: tsdat_test                  # The name of the AWS S3 bucket where processed files will be stored
			root_dir: /storage/root                  # The root dir (key) prefix for all processed files created in the bucket

-----------------------------
File Handlers 
-----------------------------
File Handlers declare the classes that should be used to read input and output files.
Correspondingly, the file_handlers section in the yaml is split into two parts for input
and output.  For input files, you can specify a Python regular expression to match
any specific file name pattern that should be read by that File Handler.

For output files, you can specify one or more formats.  Tsdat will write processed 
data files using all the output formats specified.  We recommend using the
NetCdfHandler as this is the most powerful and flexible format that will support any data.
However, other file formats may also be used such as Parquet or CSV.  More output
file handlers will be added over time.

.. code-block:: yaml
	
		file_handlers:
			input:
				sta:                          # This is a label to identify your file handler
					file_pattern: '.*\.sta'   # Use a Python regex to identify files this handler should process
					classname: pipeline.filehandlers.StaFileHandler  # Declare the fully qualified name of the handler class

			output:
				netcdf:                       # This is a label to identify your file handler
					file_extension: '.nc'     # Declare the file extension to use when writing output files
					classname: tsdat.io.filehandlers.NetCdfHandler  # Declare the fully qualified name of the handler class

.. _pipeline_config:

=============================
Pipeline Config
=============================
The pipeline config file is used to define how the pipeline will standardize input data.
It defines all the pieces of your standardized dataset, as described in the in the 
`Data Standards Document <https://github.com/ME-Data-Pipeline-Software/data_standards>`_.
Specifically, it identifies the following components:

#. **Global attributes** - dataset metadata
#. **Dimensions** - shape of data
#. **Coordinate variables** - coordinate values for a specific dimension
#. **Data variables** - all other variables in the dataset
#. **Quality management** - quality tests to be performed for each variable and any associated corrections to be applied for failing tests.

Each pipeline template will include a starter pipeline config file in the config folder.
It will work out of the box, but the configuration should be tweaked according to the
specifics of your dataset.

A full annotated example of an ingest pipeline config file is provided below and 
can also be referenced in the
`Tsdat Repository <https://github.com/tsdat/tsdat/blob/master/examples/templates/ingest_pipeline_template.yml>`_
 
.. literalinclude:: figures/ingest_pipeline_template.yml
    :linenos:
    :language: yaml

********************
Code Hooks
********************
This section describes all the types of classes that can be extended in Tsdat to provide
custom pipeline behavior.  To start with, each pipeline will define a main Pipeline class
which is used to run the pipeline itself.  Each pipeline template will come with a Pipeline 
class pre-defined in the pipeline/pipeline.py file.  The Pipeline class extends a specific base class depending upon the
template that was selected.  Currently, we only support one pipeline base class, ``tsdat.pipeline.ingest_pipeline.IngestPipeline``.
Later, support for VAP pipelines will be added.  Each pipeline base class provides certain abstract methods which
the developer can override if desired to customize pipeline functionality.  In your template repository,
your Pipeline class will come with all the hook methods stubbed out automatically (i.e., they will be 
included with an empty definition).  Later as more templates are added - in particular to support
specific data models- hook methods may be pre-filled out to implement prescribed calculations.

In addition to your Pipeline class, additional classes can be defined to provide specific behavior
such as unit conversions, quality control tests, or reading/writing files.  This section lists all
of the custom classes that can be defined in Tsdat and what their purpose is.

.. note::
   For more information on classes in Python, see `<https://docs.python.org/3/tutorial/classes.html>`_

.. note::
   We warmly encourage the community to contribute additional support classes such as FileHandlers and
   QCCheckers.

=============================
IngestPipeline Code Hooks
=============================

The following hook methods (which can be easily identified because they all start with the 'hook\_' prefix) 
are provided in the IngestPipeline template.  They are listed in the order that they are
executed in the pipeline.

hook_customize_raw_datasets
   Hook to allow for user customizations to one or more raw xarray Datasets
   before they merged and used to create the standardized dataset.  This 
   method would typically only be used if the user is combining
   multiple files into a single dataset.  In this case, this method may
   be used to correct coordinates if they don't match for all the files,
   or to change variable (column) names if two files have the same
   name for a variable, but they are two distinct variables.

   This method can also be used to check for unique conditions in the raw
   data that should cause a pipeline failure if they are not met.

   This method is called before the inputs are merged and converted to
   standard format as specified by the config file.

hook_customize_dataset
   Hook to allow for user customizations to the standardized dataset such
   as inserting a derived variable based on other variables in the
   dataset.  This method is called immediately after the apply_corrections
   hook and before any QC tests are applied.

hook_finalize_dataset
   Hook to apply any final customizations to the dataset before it is
   saved. This hook is called after quality tests have been applied.

hook_generate_and_persist_plots
   Hook to allow users to create plots from the xarray dataset after 
   processing and QC have been applied and just before the dataset is saved to disk.


********************
File Handlers
********************
File Handlers are classes that are used to read and write files.  Each File Handler
should extend the ``tsdat.io.filehandlers.file_handlers.AbstractFileHandler`` base
class.  The AbstractFileHandler base class defines two methods:

read
   Read a file into an XArray Dataset object.

write
   Write an XArray Dataset to file.  This method only needs to be implemented for
   handlers that will be used to save processed data to persistent storage.

Each pipeline template comes with a default custom FileHandler implementation
to use as an example if needed.  In addition, see the  
`ImuFileHandler <https://github.com/tsdat/tsdat/blob/master/examples/a2e_imu_ingest/pipeline/imu_filehandler.py>`_
for another example  of writing a custom FileHandler to read raw instrument data.

The File Handlers that are to be used in your pipeline are configured in your
:ref:`storage config file<storage_config>`

********************
Quality Management
********************
Two types of classes can be defined in your pipeline to ensure standardized
data meets quality requirements: 

QualityChecker
   Each QualityChecker performs a specific QC test on one or more variables
   in your dataset. 

QualityHandler
   Each QualityHandler can be specified to run if a particular QC test fails.  It
   can be used to correct invalid values, such as interpolating to fill gaps in
   the data.

The specific QCCheckers and QCHandlers used for a pipeline and the
variables they run on are specified in the :ref:`pipeline config file<pipeline_config>`.

===================
Quality Checkers
===================
Quality Checkers are classes that are used to perform a QC test on a specific
variable.  Each Quality Checker should extend the ``tsdat.qc.checkers.QualityChecker`` base
class, which defines a ``run()`` method that performs the check.
Each QualityChecker defined in the pipeline config file will be automatically initialized
by the pipeline and invoked on the specified variables.  See the :ref:`API Reference<api>`
for a detailed description of the QualityChecker.run() method as well as a list of all
QualityCheckers defined by Tsdat.

===================
Quality Handlers
===================
Quality Handlers are classes that are used to correct variable data when a specific
quality test fails.  An example is interpolating missing values to fill gaps.
Each Quality Handler should extend the ``tsdat.qc.handlers.QualityHandler`` base
class, which defines a ``run()`` method that performs the correction.
Each QualityHandler defined in the pipeline config file will be automatically initialized
by the pipeline and invoked on the specified variables.  See the :ref:`API Reference<api>`
for a detailed description of the QualityHandler.run() method as well as a list of all
QualityHandlers defined by Tsdat.





