.. _user_guide:

User Guide
##########

.. _terms:

Terminology
***********

Coming soon...


.. _config-files:

Configuration Files
*******************

Configuration files provide an explict way to define and customize the behavior
of tsdat data pipelines. There are two main types of configuration files –
`Pipeline Configuration Files`, and `Storage Configuration Files`. This section
breaks down the various properties of both types of configuration files and 
shows how these files can be modified to support a wide variety of data 
pipelines.

.. _pipeline-config:

Pipeline Configuration Files
----------------------------


.. _storage-config:

Storage Configuration Files
---------------------------


.. _file-handlers:

File Handlers
*************
The FileHandler class provides a customizable interface between files entering 
and leaving the tsdat framework, which allows the framework to be 
file-format-agnostic on both its input and its output. FileHandlers can be 
registered with specific file type(s) or to match a specified regular 
expression on the filename. The only requirement for FileHandlers is that they
implement read and write methods. A simple FileHandler for netCDF files is 
defined below:

.. code-block:: python

    import tsdat
    import xarray as xr

    class NetCdfFileHandler(tsdat.io.AbstractFileHandler):
        def read(filename: str, *args, **kwargs) -> xr.Dataset:
            return xr.open_dataset(filename)
        
        def write(filename: str, dataset: xr.Dataset, *args, **kwargs):
            dataset.to_netcdf(filename)


.. _quality-management:

Quality Management
******************


.. _quality-checkers:

Quality Checkers
----------------

Quality Checkers are tsdat's primary mechanism for the detection of quality 
problems in processed datasets. Quality Checkers are python classes that 
operate on xarray dataarrays and return a N-D array of quality results (
``True`` if the value has failed the quality check, ``False`` otherwise) with 
the same shape as the underlying data it is applied to. Quality Checkers are 
registered in the pipeline configuration file. They are applied on a 
variable-by-variable basis as defined in the pipeline configuration file.


.. _quality-handlers:

Quality Handlers
----------------

Quality Handlers run after Quality Checkers and are provided with the array of
quality results (boolean flags) from a Quality Checker. Quality Handlers are 
expected to use this information and take some action – whether that be to 
simply record the quality results in a separate variable, apply some code to 
correct the value, or do something else entirely. 

Tsdat comes packaged with several Quality Handlers out of the box to support 
common use cases, but for more complex data pipelines, a custom quality handler
may be warrented. See the :ref:`custom-quality-handlers` section for more 
details on how to create your own quality handler.



.. _customizing-tsdat:

Customizing tsdat
*****************



.. _ingest-pipeline-hooks:

Custom Code Hooks: IngestPipeline
---------------------------------



.. _custom-pipelines:

Custom Pipelines
----------------



.. _custom-quality-checkers:

Custom Quality Checkers
-----------------------



.. _custom-quality-handlers:

Custom Quality Handlers
-----------------------

