.. _quality_control:

Notes on Tsdat's Quality Control Methods
----------------------------------------

In Tsdat, all variables are given a corollary quality control (QC) variable if 
QC tests for coordinates or data variables are run in a given pipeline. What this
means that is data variables will not get a corollary QC variable if the respective 
QC blocks for "DATA_VARS" are commented out. The basic quality.yaml file that is 
given in the template is shown below:

.. code-block:: yaml

    managers:
    #---------------------------------------------------------------
    - name: Fail if missing coordinates
        checker:
        classname: tsdat.qc.checkers.CheckMissing
        handlers:
        - classname: tsdat.qc.handlers.FailPipeline
            parameters:
              context: Coordinate variables cannot be missing.
        apply_to:
        - COORDS

    - name: Fail if monotonic coordinates
        checker:
        classname: tsdat.qc.checkers.CheckMonotonic
        parameters:
          require_increasing: true
        handlers:
        - classname: tsdat.qc.handlers.FailPipeline
            parameters:
              context: Coordinate variables must be strictly increasing.
        apply_to:
        - COORDS

        #---------------------------------------------------------------
    - name: Remove missing data
        checker:
        classname: tsdat.qc.checkers.CheckMissing
        handlers:
        - classname: tsdat.qc.handlers.RemoveFailedValues
        - classname: tsdat.qc.handlers.RecordQualityResults
            parameters:
              bit: 1
              assessment: bad
              meaning: "Value is equal to _FillValue or NaN"
        apply_to:
        - DATA_VARS

    - name: Flag data below minimum valid threshold
        checker:
        classname: tsdat.qc.checkers.CheckValidMin
        handlers:
        - classname: tsdat.qc.handlers.RecordQualityResults
            parameters:
              bit: 2
              assessment: bad
              meaning: "Value is less than the valid_min."
        apply_to:
        - DATA_VARS
    
    - name: Flag data above maximum valid threshold
        checker:
        classname: tsdat.qc.checkers.CheckValidMax
        handlers:
        - classname: tsdat.qc.handlers.RecordQualityResults
            parameters:
              bit: 3
              assessment: bad
              meaning: "Value is greater than the valid_max."
        apply_to:
        - DATA_VARS


A QC block consists of 
    1. the keyword "name", simply the block's description
    2. the keyword "checker", and an associated "classname" one line below it: the QC test to use.
    3. the keyword "handler", and an associated list of "classname"(s) (hence the extra hyphen in front of "classname")
    4. the keyword "apply_to": this can be "COORDS", "DATA_VARS", or a list of variable names


These QC blocks can take additional parameters for more sophisticated QC algorithms. 
Customized "qc.py" files that require editable parameters can be set using the "parameters"
keyword. In the QC block below, the QC class "CheckCorrelation" exists in the 
"<pipeline_name>/shared/qc.py" file. The classname is therefore set as 
"shared.qc.CheckCorrelation".

The "exclude" keyword can be used to exclude certain variables from a QC test,
and is typically needed for variables that are not numeric, i.e. chars and strings.

Also know that whitespace is not critical for yaml files, and it is good to be consistent
with however yours is set.

.. code-block:: yaml

    - name: Flag data below correlation threshold
        checker:
        classname: shared.qc.CheckCorrelation
        parameters:
          correlation_threshold: 30
        handlers:
        - classname: tsdat.qc.handlers.RemoveFailedValues
        - classname: tsdat.qc.handlers.RecordQualityResults
            parameters:
              bit: 4
              assessment: bad
              meaning: "Value is less than correlation threshold"
        apply_to: [vel, corr, amp]
        exclude: [vel_bt]


Finally, it's important to go over the parameters required for ``RecordQualityResults``,
which is the built-in function that all QC blocks should use to record the QC test results.

It takes 4 parameters: "bit", "assessment", and "meaning". These parameters are turned into
variable attributes in the pipeline output dataset: "flag_mask", "flag_assessment", and "flag_meaning",
respectively. 

"Bit" is shorthand for the QC bit, which is defined sequentially starting from 
1 to "n", depending on how many tests a pipeline has. The "flag_mask" is calculated as 
2^{bit-1}. So for the bits 1, 2, 3, and 4, the associated flag masks will be 1, 2, 4, and 8.
If a flag has the value 13, then that means it failed the tests associated with flag masks 
1, 4, and 8 (1 + 4 + 8 = 13), which are QC bits 1, 3, and 4. This scheme works because 
any addition of the flag masks can only come from a unique set of QC bits.

"Assessment" is one of two terms: "bad" or "indeterminate". This simply flags if the test 
that failed did so because the datapoint is of bad quality or if it may be cause for concern.

"Meaning" is the description of the failure. This is a short statement of which test failed,
and "flag_meaning" is listed in the same order as "flag_masks".
