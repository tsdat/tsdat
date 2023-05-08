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
	
Custom QC Checkers and QC Handlers are stored (typically) in ``pipelines/<pipeline_module>/qc.py``.
Once written, they must be specified in the ``config/quality.yaml`` file like shown:

.. code-block:: yaml

    managers:
      - name: Require Valid Coordinate Variables
        checker:
          classname: tsdat.qc.checkers.CheckMissing
        handlers:
          - classname: tsdat.qc.handlers.FailPipeline
        apply_to: [COORDS]

      - name: The name of this quality check
        checker:
          classname: pipelines.example_pipeline.qc.CustomQualityChecker
          parameters: {}
        handlers:
          - classname: pipelines.example_pipeline.qc.CustomQualityHandler
            parameters: {}
        apply_to: [COORDS, DATA_VARS]


Quality Checkers
----------------
Quality Checkers are classes that are used to perform a QC test on a specific
variable.  Each Quality Checker should extend the ``QualityChecker`` base
class, and implement the abstract ``run`` method as shown below.  Each QualityChecker
defined in the pipeline config file will be automatically initialized by the pipeline
and invoked on the specified variables.

.. code-block:: python

    @abstractmethod
    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool_]:
        """-----------------------------------------------------------------------------
        Checks the quality of a specific variable in the dataset and returns the results
        of the check as a boolean array where True values represent quality problems and
        False values represent data that passes the quality check.

        QualityCheckers should not modify dataset variables; changes to the dataset
        should be made by QualityHandler(s), which receive the results of a
        QualityChecker as input.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to check.
            variable_name (str): The name of the variable to check.

        Returns:
            NDArray[np.bool_]: The results of the quality check, where True values
            indicate a quality problem.

        -----------------------------------------------------------------------------"""



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
class, and implement the abstract `run` method that performs the correction, as shown below.
Each QualityHandler defined in the pipeline config file will be automatically initialized
by the pipeline and invoked on the specified variables.

.. code-block:: python

    @abstractmethod
    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool_]
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Handles the quality of a variable in the dataset and returns the dataset after
        any corrections have been applied.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to handle.
            variable_name (str): The name of the variable whose quality should be
                handled.
            failures (NDArray[np.bool_]): The results of the QualityChecker for the
                provided variable, where True values indicate a quality problem.

        Returns:
            xr.Dataset: The dataset after the QualityHandler has been run.

        -----------------------------------------------------------------------------"""


Tsdat built-in quality handlers:

.. autosummary::
    :nosignatures:
	
    ~tsdat.qc.handlers.QualityHandler
    ~tsdat.qc.handlers.RecordQualityResults
    ~tsdat.qc.handlers.RemoveFailedValues
    ~tsdat.qc.handlers.SortDatasetByCoordinate
    ~tsdat.qc.handlers.FailPipeline

.. automodule:: tsdat.qc.checkers
    :members:
    :undoc-members:
    :show-inheritance:
    :noindex:
	
.. automodule:: tsdat.qc.handlers
    :members:
    :undoc-members:
    :show-inheritance:
    :noindex:
