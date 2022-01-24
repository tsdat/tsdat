.. _filehandlers:

File Handlers 
=============

File Handlers declare the classes that should be used to read raw input 
and write final output files.

For input files, you can specify a Python regular expression to match
any specific file name pattern that should be read by that File Handler. 
A custom filehandler can contain any level of pre-analysis that the user 
desires; the only requirement is that it returns an xarray Dataset.

For output files, you can specify one or more formats. Tsdat will write 
processed data files using all the output formats specified.

Custom file handlers are stored in (typically) ``ingest/<ingest_name>/pipeline/filehandler.py``.
Once written, they must be specified in the ``storage_config.yml`` file 
like shown:

.. code-block:: yaml

  file_handlers:
    input:
      custom:   # Label to identify your file handler
        file_pattern: ".*.ext"   # Use a Python regex to identify files this handler should process
        classname: ingest.<ingest_name>.pipeline.filehandlers.CustomHandler   # Declare the fully qualified name of the handler class
        parameters:   # Parameters provided to filehandler function
          threshold: 50   # Parameter name and value (accessed in filehandler function via `self.parameters.get(<param_name>)`)
      
      # Tsdat built-in csv file handler and parameter keywords 
      csv:
        file_pattern: ".*.csv"
        classname: tsdat.io.handers.CsvHandler
        parameters:
          read:
            read_csv: # pandas.read_csv arguments
              sep: ","
              header: 0
              index_col: False
              
      # Tsdat built-in netcdf file handler and parameter keywords 
      netcdf:
        file_pattern: ".*.nc"
        classname: tsdat.io.handers.NetCdfHandler
        parameters:
          read:
            load_dataset: # xarray.load_dataset arguments
              engine: "netcdf4"


    # Tsdat built-in output filetypes
    output:
      netcdf:
        file_extension: ".nc"  # Declare the file extension to use when writing output files
        classname: tsdat.io.handlers.NetCdfHandler
        
      csv:
        file_extension: ".csv"
        classname: tsdat.io.handers.CsvHandler


Tsdat natively handles csv and netcdf file formats:

.. autosummary::
	:nosignatures:
	
  ~tsdat.io.handlers.csv.CsvHandler
	~tsdat.io.handlers.netcdf.NetCdfHandler
  

.. automodule:: tsdat.io.handlers.csv
    :members:
    :undoc-members:
    :show-inheritance:
	
.. automodule:: tsdat.io.handlers.netcdf
    :members:
    :undoc-members:
    :show-inheritance: