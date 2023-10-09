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
      classname: tsdat.CheckMissing
    handlers:
      - classname: tsdat.FailPipeline
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
    """Checks the quality of a specific variable in the dataset and returns the results
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
  return None
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
    return dataset
```

## Additional Notes

Using the default template, all variables are given a corollary quality control (QC) variable if QC tests for
data variables are run in a given pipeline. What this means that is data variables will not get a corollary QC variable
if the respective QC blocks for `DATA_VARS` are commented out. The basic `quality.yaml` file that is given in the
template is shown below:

```yaml title="shared/quality.yaml"
managers:
- name: Fail if missing coordinates
    checker:
    classname: tsdat.CheckMissing
    handlers:
    - classname: tsdat.FailPipeline
        parameters:
          context: Coordinate variables cannot be missing.
    apply_to: [COORDS]

- name: Fail if monotonic coordinates
    checker:
    classname: tsdat.CheckMonotonic
    parameters:
      require_increasing: true
    handlers:
    - classname: tsdat.FailPipeline
        parameters:
          context: Coordinate variables must be strictly increasing.
    apply_to: [COORDS]

#---------------------------------------------------------------
- name: Remove missing data
    checker:
    classname: tsdat.CheckMissing
    handlers:
    - classname: tsdat.RemoveFailedValues
    - classname: tsdat.RecordQualityResults
        parameters:
          assessment: bad
          meaning: Value is equal to _FillValue or NaN
    apply_to: [DATA_VARS]
    exclude:
      - altitude
      - latitude
      - longitude

- name: Flag data below minimum valid threshold
    checker:
    classname: tsdat.CheckValidMin
    handlers:
    - classname: tsdat.RecordQualityResults
        parameters:
          assessment: bad
          meaning: Value is less than the valid_min.
    apply_to: [DATA_VARS]
    exclude:
      - altitude
      - latitude
      - longitude

- name: Flag data above maximum valid threshold
    checker:
    classname: tsdat.CheckValidMax
    handlers:
    - classname: tsdat.RecordQualityResults
        parameters:
          assessment: bad
          meaning: Value is greater than the valid_max.
    apply_to: [DATA_VARS]
    exclude:
      - altitude
      - latitude
      - longitude
```

A QC block consists of

1. the keyword `name`, simply the block's description
2. the keyword `checker`, and an associated `classname` one line below it: the QC test to use.
3. the keyword `handler`, and an associated list of classname(s) (hence the extra hyphen in front of `classname`)
4. the keyword `apply_to`: this can be `COORDS`, `DATA_VARS`, or a list of variable names
5. the keyword `exclude`: this is a list of variable names that the check should ignore

These QC blocks can take additional parameters for more sophisticated QC algorithms. Customized `qc.py` files that
require editable parameters can be set using the parameters keyword. In the QC block below, the custom QC class
`CheckCorrelation` exists in the `shared/qc.py` file. The classname is therefore set as `shared.qc.CheckCorrelation`.

The `exclude` keyword can be used to exclude certain variables from a QC test, and is typically used for variables that
you don't want to QC (e.g., location variables or other status variables).

Also know that whitespace is not critical for `yaml` files, and it is good to be consistent with however yours is set.

```yaml
- name: Flag data below correlation threshold
    checker:
    classname: shared.qc.CheckCorrelation
    parameters:
      correlation_threshold: 30
    handlers:
    - classname: tsdat.RemoveFailedValues
    - classname: tsdat.RecordQualityResults
        parameters:
          assessment: bad
          meaning: Value is less than correlation threshold
    apply_to: [vel, corr, amp]
    exclude: [vel_bt]
```

Finally, it's important to go over the parameters required for `tsdat.RecordQualityResults`, which is the built-in
function that all QC blocks should use to record the QC test results.

It takes parameters: `assessment` and `meaning`. These parameters are turned into variable attributes in the pipeline
output dataset on each `qc` variable: `flag_mask`, `flag_assessment`, and `flag_meaning` respectively.

A technique called bit-packing is used to create the flag masks attribute. Each bit/flag represents one test that was
applied (e.g., each time `tsdat.RecordQualityResults` is called). If a flag has the value 13, then that means it failed
the tests associated with flag masks 1, 4, and 8 (1 + 4 + 8 = 13), which are QC bits 1, 3, and 4 (2^0, 2^2, 2^3). This
scheme works because any addition of the flag masks can only come from a unique set of QC bits.

`assessment` is one of two terms: bad or indeterminate. This simply flags if the test that failed did so because the
data point is of bad quality or if it may be cause for concern.

`meaning` is the description of the failure. This is a short statement of which test failed, and `flag_meaning` is
listed in the same order as `flag_masks`.
