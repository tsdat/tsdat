# Quality Management

Two types of classes can be defined in your pipeline to ensure standardized data meets quality requirements:

`QualityChecker`

:   Each Quality Checker performs a specific quality control (QC) test on one or more variables in your dataset. Quality
    checkers test a single data variable at a time and return a boolean mask, where flagged values are marked as `True`.

`QualityHandler`

:   Each Quality Handler can be specified to run if a particular QC test fails. Quality handlers take the QC Checker's
    boolean mask and use it to apply any QC or custom method to the data variable of question. For instance, it can be
    used to remove flagged data altogether or correct flagged values, such as interpolating to fill gaps in data.

Custom Quality Checkers and Handlers should be stored in the folder where they will be used (i.e.,
`pipelines/<pipeline_module>/qc.py` in most cases). In order to be used, they must be registered in the
quality config file like so (for an example `lidar` pipeline module):

```yaml title="pipelines/lidar/config/quality.yaml"
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
```

## Quality Checkers

Quality Checkers are classes that are used to perform a QC test on a specific variable. Each `QualityChecker` should
extend the `tsdat.QualityChecker` base class, and implement the `run` method as shown below. Each `QualityChecker`
registered in the pipeline config file will be automatically initialized by the pipeline and invoked on the
specified variables.

```python
def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool_] | None:
    """
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
        NDArray[np.bool_] | None: The results of the quality check, where True values
        indicate a quality problem. May return None to indicate that no QualityHandlers
        should be run on the results of this check.

    """
```

## Quality Handlers

Quality Handlers are classes that are used to take some action following the result of a quality check. For example, if
you use the built-in `tsdat.CheckMissing` `QualityChecker` class to check for missing values in one or more of your data
variables, you could pair that with the `tsdat.RecordQualityResults` `QualityHandler` to add a new `qc` variable that
provides metadata to indicate to users which data points are missing.

All `QualityHandler` classes should extend the `tsdat.QualityHandler` base class, and implement the `run()` method as
shown below:

```python
def run(
    self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool_]
) -> xr.Dataset:
    """Handles the quality of a variable in the dataset and returns the dataset after
    any corrections have been applied.

    Args:
        dataset (xr.Dataset): The dataset containing the variable to handle.
        variable_name (str): The name of the variable whose quality should be
            handled.
        failures (NDArray[np.bool_]): The results of the QualityChecker for the
            provided variable, where True values indicate a quality problem.

    Returns:
        xr.Dataset: The dataset after the QualityHandler has been run.
    """
```
