.. _more_code:

Readers, Quality Control, Etc
-----------------------------------

This quick walkthrough shows how to add custom quality control and data reader code to
tsdat for the `pipeline-template`. See :ref:`first tutorial <data_ingest>` to learn how
to set up an ingest first if you haven't already.

After running the command 

.. code-block::

	cookiecutter templates/ingest -o ingest/
  
to create a new ingest, fill out the information and type "1" to the prompts to add
custom readers and quality control functions.

This will create additional ``readers.py`` and ``qc.py`` files under the ``pipelines/<ingest_name>`` directory.

  .. figure:: global_marine_data/vscode25.png
      :align: center
      :width: 100%
      :alt:

  |

Adding a Custom Reader
============================

The ``readers.py`` file contains a stubbed out custom class to read in your particular datafile.
The class name, shown as `CustomDataReader` below, can be whatever you like. It is recommended
to test your code before inputting to tsdat's framework. Your code will sit
under the ``read`` method within this class, and should return an xarray Dataset.

.. code-block:: python

    import xarray as xr
    from tsdat import DataReader


    class CustomDataReader(DataReader):
        """---------------------------------------------------------------------------------
        Data reader that can read from *xyz* formatted-data files.

        ---------------------------------------------------------------------------------"""

        # DEVELOPER: Implement the read function update the classname/docstring as needed.

        def read(self, input_key: str) -> xr.Dataset:
            raise NotImplementedError
            return xr.Dataset()


After adding your custom reader code, you need to tell tsdat to use the custom reader you
just added, which is done in the ``retriever.yaml`` file. Add a new entry under `readers`, with
they key being a regular expression indicating the file name patterns that will use your reader.
If you use ``.*`` as shown below, then all input files will use this custom reader.  If your reader
takes custom parameters, then add an additional parameters section to your reader as shown:

.. code-block:: yaml

    readers:
      .*:
        classname: pipelines.my_ingest.readers.CustomDataReader
        parameters:
          header_lines: 6


Tsdat's Native Readers
============================

Tsdat has two native file readers: ``CsvReader`` and ``NetcdfReader``.
The ``CsvReader`` uses ``pandas.read_csv`` to read in a .csv file, and the
``NetcdfReader`` uses ``xarray.load_dataset`` to read a .nc file. These should
be configured like that shown in :ref:`configuring the retriever <retriever_config>`
with the specific format of your input file.


Adding Custom Quality Control Functions
=======================================

The same process is followed to add custom QC code. In the ``qc.py`` file, you can add custom
`checkers` and `handlers`. Rename the class to something descriptive, and add your qc code
to the `run` definition. QualityCheckers should return a boolean numpy array (True/False), where
`True` refers to flagged data, for each variable in the raw dataset. QualityHandlers take this boolean array
and apply some function to the data variable it was created from.

.. code-block:: python

    import numpy as np
    import xarray as xr
    from numpy.typing import NDArray
    from tsdat import QualityChecker, QualityHandler


    class CustomQualityChecker(QualityChecker):
        """---------------------------------------------------------------------------------
        Custom Quality Checker

        ---------------------------------------------------------------------------------"""

        def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:

            var_data = dataset[variable_name]

            failures: NDArray[np.bool8] = np.zeros_like(var_data, dtype=np.bool8)  # type: ignore

            # DEVELOPER: Add your custom quality checking code here
            raise NotImplementedError

            return failures


    class CustomQualityHandler(QualityHandler):
        def run(
            self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]
        ) -> xr.Dataset:

            # DEVELOPER: Add custom quality handling code here
            raise NotImplementedError

            return dataset

Similar to the file reader, you must tell tsdat where and when to use your QC code, which
is done in the ``managers`` section of the ``quality.yaml`` file. Add a descriptive name for your custom check,
update the classnames (if you changed them from the template), and add any parameters you'd like to incorporate
as shown:

.. code-block:: yaml

    managers:
      - name: The name of this quality check
        checker:
          classname: pipelines.example_ingest.qc.CustomQualityChecker
          parameters: {}
        handlers:
          - classname: pipelines.example_ingest.qc.CustomQualityHandler
            parameters: {}
        apply_to: [COORDS, DATA_VARS]


Tsdat's Native QC Functions
===========================

Tsdat has a number of native quality control functions that users could find useful. 
See :ref:`quality control API <quality_control>` for a full list and a description of their parameters.

Many of the native checkers depend upon variable attribute values defined in the ``dataset.yaml`` file.  For example
the ``Check*Max`` functions (``CheckValidMax``, ``CheckFailMax``, ``CheckWarnMax``) require
a corresponding `attribute` called ``*_range`` (``valid_range``, ``fail_range``, ``warn_range``,
respectively) to be included in the variable's definition in the dataset.yaml file.  When using the native checkers,
please ensure that the correct varible attributes are included in your dataset.yaml file.

The special ``ReplaceFailedValues`` handler removes all failed values and replaces them for with a fill value,
specified in each variable's `attribute` ``_FillValue``. If this attribute isn't
specified, it defaults to ``NaN``.

Another function of interest is ``RecordQualityResults``, which takes a few
parameters: "bit", "assessment", and "meaning". This function creates an additional, companion
variable in your output dataset called ``<variable_name>_qc``, referred to as a `qc variable`.  Qc variables have
the same shape as their parent variable, but the data values are bit masked integers.  A bit in the integer is
flipped if the corresponding quality test fails.  If no tests fail, then the qc variable will contain all zeroes.
This gives you a quick way to scan for quality issues by looking for any values in the qc variable that are > 0.

In addition to custom checkers/handlers, tsdat's built-in quality functions can be used in the `quality.yaml` file as
illustrated in the following example:

.. code-block:: yaml

    managers:
      - name: manage_minimum
        checker:
          classname: tsdat.qc.checkers.CheckValidMin
        handlers:
          - classname: tsdat.qc.handlers.ReplaceFailedValues
          - classname: tsdat.qc.handlers.RecordQualityResults
            parameters:
              bit: 2
              assessment: Bad
              meaning: "Value is less than expected range"

        apply_to: [DATA_VARS]


Notes on Errors
===============

Errors commonly ensue from data file located in incorrect directories, incorrect 
"classname" paths, and syntax errors. If you get an error, most of the time it will be caused by an invalid
input file or an invalid configuration in one of the yaml files.

Common Errors:

  1. KeyError ['time'] -- Time is typically the first variable tsdat looks
  for, so if it can't load your dataset or if the time coordinate is not input 
  correctly, this error will pop up. The failure load a dataset typically results 
  from incorrect file extensions, regex patterns, or file path location.
  
  2. Can't find module -- This error typically refers to a custom classname specified in one of the yaml
  config files (i.e. ``pipelines.<ingest_name>.qc.CustomQualityChecker``). Please make sure your classname paths
  are correct.
  
  3. ``Check_<function>`` fails -- Ensure all the variables listed under a quality 
  managment group can be run through the function. For example, if I try to run the  
  test ``CheckMonotonic`` on all "COORDS", and one of my coordinate variables is a
  string array (e.g 'direction': ['x','y','z'], this function will fail. Fix this by
  replacing "COORDS" with only numeric coordinates (e.g. 'time').
