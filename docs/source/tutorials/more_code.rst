.. _more_code:

File Handlers, Quality Control, Etc
-----------------------------------

This quick walkthrough shows how to add custom quality control and file handler code to 
tsdat for the `pipeline-template`. See :ref:`first tutorial <data_ingest>` to learn how
to set up an ingest first if you haven't already.

After running the command 

.. code-block::

	cookiecutter templates/ingest -o ingest/
  
to create a new ingest, fill out the information and type "1" to the prompts to add
custom file handlers and quality control functions.

This will create additional ``filehandler.py`` and ``qc.py`` files under the ``ingest/<ingest_name>/pipeline/`` directory.

  .. figure:: global_marine_data/vscode25.png
      :align: center
      :width: 100%
      :alt:

  |

Adding a Custom File Handler
============================

First, the ``filehandler.py`` file that contains the code to read in your particular datafile.
The class name, shown as `CustomFileHandler` below, can be whatever you like. It is recommended
to test your code before inputting to tsdat's framework. Your code will sit
under the ``read`` definition within this class, and should return an xarray Dataset.

.. code-block:: python

  class CustomFileHandler(tsdat.AbstractFileHandler):
      """
      Custom file handler for reading <some data type> files from a <instrument name>.

      See https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/io/index.html for more
      examples of FileHandler implementations.
      """

      def read(self, filename: str, **kwargs) -> xarray.Dataset:
          """
          Method to read data in a custom format and convert it into an xarray Dataset.

          Args:
              filename (str): The path to the file to read in.

          Returns:
              xarray.Dataset: An xarray.Dataset object
          """
          
          threshold = self.parameters['threshold']
          raw_data = read_function(filename, threshold) 

          return raw_data # an xarray Dataset

After adding your custom file handler code, you need to tell tsdat to use your custom code you
just added, which is done in the ``storage_config.yml`` file. Add a new entry under `input`, with
a short label, add the file entension under `file_pattern`, and the classname path. An inputs
required for functions can be added under the `parameters` tag.

.. code-block:: yaml

  file_handlers:
    input:

      custom:   # Label to identify your file handler
        file_pattern: ".*.ext"
        classname: ingest.<ingest_name>.pipeline.filehandlers.CustomHandler
        parameters: 
          threshold: 50  # any inputs desired fall under the parameters list

Tsdat's Native File Handlers
============================

Tsdat has two native filehandlers: ``CsvHandler`` and ``NetcdfHandler``.
The ``CsvHandler`` uses ``pandas.read_csv`` to read in a .csv file, and the 
``NetcdfHandler`` uses ``xarray.load_dataset`` to read a .nc file. These should 
be configured like that shown in :ref:`configuring file handlers <filehandlers>` 
with the specific format of your input file.


Adding Custom Quality Control Funtions
======================================

The same process is followed to add custom QC code. In the ``qc.py`` file, you can add custom
`checkers` and `handlers`. Rename the class to something descriptive, and add your qc code
to the `run` definition. QualityCheckers should return a boolean numpy array (True/False), where
`True` refers to flagged data, for each variable in the raw dataset. QualityHandlers take this boolean array and apply some function to the data variable it was created from.

.. code-block:: python

  from tsdat import DSUtil, QualityChecker, QualityHandler

  class CustomQualityChecker(QualityChecker):
      def run(self, variable_name: str) -> Optional[np.ndarray]:
          """
          False values in the results array mean the check passed, True values indicate
          the check failed. Here we initialize the array to be full of False values as
          an example. Note the shape of the results array must match the variable data.
          """
          
          npt=self.params["n_points"]
          results_array = qc_function(self.ds[variable_name].data, npt) # returns boolean numpy array

          return results_array

  class CustomQualityHandler(QualityHandler):
      def run(self, variable_name: str, results_array: np.ndarray):
          """
          Some QualityHandlers only want to run if at least one value failed the check.
          In this case, we replace all values that failed the check with the variable's
          _FillValue and (possibly) add an attribute to the variable indicating the
          correction applied.
          """
          
          if results_array.any():

              fill_value = DSUtil.get_fill_value(self.ds, variable_name)
              keep_array = np.logical_not(results_array)

              var_values = self.ds[variable_name].data
              replaced_values = np.where(keep_array, var_values, fill_value)
              self.ds[variable_name].data = replaced_values

              self.record_correction(variable_name)

Likewise to the file handler, you must tell tsdat where and when to use your QC code, which
is done in the `quality_management` section of the ``pipeline_config.yml`` file, similar to as
follows. Add a descriptive group label, and update the classnames, as well as any parameters you'd
like to incorporate:

.. code-block:: yaml

  quality_management:

    custom_QC_name: # Label to identify your QC check
      checker:
        classname: ingest.<ingest_name>.pipeline.qc.CustomQualityChecker
        parameters:
          npt: 1000
      handlers:
        - classname: ingest.<ingest_name>.pipeline.qc.CustomQualityHandler
        - classname: tsdat.qc.handlers.RecordQualityResults  # Built-in tsdat error logging
          parameters:
            bit: 1
            assessment: Bad
            meaning: "Flagged by custom quality checker"
      variables:
        - DATA_VARS


Tsdat's Native QC Functions
===========================

Tsdat has a number of native quality control functions that users could find useful. 
(See :ref:`quality control API <quality_control>` for all of them). Built-in QC  
funtions require inputs that are set either as `attributes` or `parameters` in 
``pipeline_config.yml``.

For example, the ``Check*Max`` functions (``CheckValidMax``, ``CheckFailMax``, 
``CheckWarnMax``) call the base class ``CheckMax``. These three functions require 
an `attribute` called ``*_range`` (``valid_range``, ``fail_range``, ``warn_range``,
respectively) to be listed in a variable's attributes to run.

``RemoveFailedValues`` removes failed values and replaces them for with a fill value, 
specified in the variable `attribute` ``_FillValue``. If this attribute isn't
specified, it defaults to ``NaN``.

.. code-block:: yaml

  dataset_definition:
    <...>
    variables:
      <...>
      
      distance:
        input:
          name: distance_m
        dims:
          [time]
        type: float
        attrs:
          units: "m"
          valid_range: [-3, 3] # attribute for the "CheckValidMin" and "CheckValidMax" functions
          _FillValue: 999


These built-in functions can then be input under the `quality_management` section as follows:

.. code-block:: yaml

  quality_management:
   
    manage_min: # tsdat's built-in handle min
      checker:
        classname: tsdat.qc.checkers.CheckValidMin
      handlers:
        - classname: tsdat.qc.handlers.RemoveFailedValues
        - classname: tsdat.qc.handlers.RecordQualityResults
          parameters:
            bit: 2
            assessment: Bad
            meaning: "Value is less than expected range"
      variables:
        - distance

    manage_max: # tsdat's built-in max
      checker:
        classname: tsdat.qc.checkers.CheckValidMax
      handlers:
        - classname: tsdat.qc.handlers.RemoveFailedValues
        - classname: tsdat.qc.handlers.RecordQualityResults
          parameters:
            bit: 3
            assessment: Bad
            meaning: "Value is greater than expected range"
      variables:
        - distance

Another function of interest is ``RecordQualityResults``, which takes a few 
parameters: "bit", "assessment", and "meaning". This function creates an additional 
variable that is called ``<variable_name>_qc``, which contains integers, where 
variable elements that fail a test are given the bit value. If no test fails, 
``<variable_name>_qc`` will contain all zeroes. The other two parameters are listed 
as ``<variable_name>_qc`` attributes.


Notes on Errors
===============

Errors commonly ensue from data file located in incorrect directories, incorrect 
"classname" paths, and syntax errors. If you get an error, most of the time there is an error,
missing or incorrect input in the "config.yml" files. 

Common Errors:

  1. KeyError ['time'] -- Time is typically the first variable tsdat looks
  for, so if it can't load your dataset or if the time coordinate is not input 
  correctly, this error will pop up. The failure load a dataset typically results 
  from incorrect file extensions, regex patterns, or file path location.
  
  2. Can't find module "pipeline" -- There are many modules and classes named 
  "pipeline" in tsdat. This error typically refers to a classname specified in the  
  config file, i.e. ``ingest.<ingest_name>.pipeline.qc.CustomQualityChecker`` or
  ``ingest.<ingest_name>.pipeline.filehandlers.CustomHandler``. Make sure this classname path is correct.
  
  3. ``Check_<function>`` fails -- Ensure all the variables listed under a quality 
  managment group can be run through the function. For example, if I try to run the  
  test ``CheckMonotonic`` on all "COORDS", and one of my coordinate variables is a
  string array (e.g 'direction': ['x','y','z'], this function will fail. Fix this by
  replacing "COORDS" with only numeric coordinates (e.g. 'time').
