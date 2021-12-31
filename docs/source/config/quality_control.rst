.. _quality_control:

Quality Management
==================

Two types of classes can be defined in your pipeline to ensure standardized
data meets quality requirements: 

:QualityChecker: 
	Each Quality Checker performs a specific quality control (QC) test on one or more variables 
	in your dataset. Quality checkers test a single data variable at a time and return a logical mask, where flagged values are marked as 'True'.

:QualityHandler: 
	Each Quality Handler can be specified to run if a particular QC test 
	fails. Quality handlers take the QC Checker's logical mask and use it to apply any QC or custom method to the data variable of question. For instance, it can be used to remove flagged data altogether or correct flagged values, such as interpolating to fill gaps in data.
	
Custom QC Checkers and QC Handlers are stored (typically) in 
``ingest/<ingest_name>/pipeline/qc.py``
Once written, they must be specified in the 
``pipeline_config_<ingest_name>.yml`` file like shown:

.. code-block:: yaml

  quality_management:

    manage_missing_coordinates: # Tsdat built-in function
      checker:
        classname: tsdat.qc.checkers.CheckMissing
      handlers:
        - classname: tsdat.qc.handlers.FailPipeline
      variables:
        - time  # Coordinates to check
	  
    despiking:  # Custom QC name
      checker:
        classname: ingest.wave.pipeline.qc.GoringNikora2002  # Custom QC checker function
        parameters:
          n_points: 1000  # parameters accessed in custom function via `self.params["<param_name>"]`
      handlers:
        - classname: ingest.wave.pipeline.qc.CubicSplineInterp  # Custom QC handler function
        - classname: tsdat.qc.handlers.RecordQualityResults  # Built-in tsdat error logging
          parameters:
            bit: 4
            assessment: Bad
            meaning: "Spike"
      variables:
        - DATA_VARS       # Catch-all for all variables
      exclude: [foo, bar] # Variables to exclude from test


Quality Checkers
----------------
Quality Checkers are classes that are used to perform a QC test on a specific
variable.  Each Quality Checker should extend the ``QualityChecker`` base
class, which defines a ``run`` method that performs the check.
Each QualityChecker defined in the pipeline config file will be automatically initialized
by the pipeline and invoked on the specified variables.

Tsdat built-in quality checkers:

.. autosummary::
	:nosignatures:
	
	~tsdat.qc.checkers.QualityChecker
	~tsdat.qc.checkers.CheckMissing
	~tsdat.qc.checkers.CheckMonotonic
	~tsdat.qc.checkers.CheckValidDelta
	~tsdat.qc.checkers.CheckValidMin
	~tsdat.qc.checkers.CheckValidMax
	~tsdat.qc.checkers.CheckFailMin
	~tsdat.qc.checkers.CheckFailMax
	~tsdat.qc.checkers.CheckWarnMin
	~tsdat.qc.checkers.CheckWarnMax


Quality Handlers
----------------
Quality Handlers are classes that are used to correct variable data when a specific
quality test fails.  An example is interpolating missing values to fill gaps.
Each Quality Handler should extend the ``QualityHandler`` base
class, which defines a `run` method that performs the correction.
Each QualityHandler defined in the pipeline config file will be automatically initialized
by the pipeline and invoked on the specified variables.

Tsdat built-in quality handlers:

.. autosummary::
	:nosignatures:
	
	~tsdat.qc.handlers.QualityHandler
	~tsdat.qc.handlers.RecordQualityResults
	~tsdat.qc.handlers.RemoveFailedValues
	~tsdat.qc.handlers.SortDatasetByCoordinate
	~tsdat.qc.handlers.SendEmailAWS
	~tsdat.qc.handlers.FailPipeline

.. automodule:: tsdat.qc.checkers
    :members:
    :undoc-members:
    :show-inheritance:
	
.. automodule:: tsdat.qc.handlers
    :members:
    :undoc-members:
    :show-inheritance:
	