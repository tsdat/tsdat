.. quality_control:

Quality Control
###############

Tsdat offers several quality control and quality assurance methods that can be used to perform quality control and 
quality assurance measures on datasets processed by tsdat. 


QC Operators
------------

QC Operators are tsdat's primary mechanism for automating the detection of quality problems in processed datasets. QC
Operators are defined in tsdat/qc/operators.py and are applied on a variable-by-variable basis as defined in the config
file. QC Operators have access to the underlying dataset and are intended only for the detection of quality problems. 
They are expected to return a boolean array with the same shape as the data, where True flags are used to indicate data
quality problems at a particular point.

.. TODO: Dicussion on bit packing 

QC Error Handlers
-----------------

QC Error handlers run after QC Operators and are provided with an array of flags where True indicates a data quality 
issue. Error handlers are expected to use this information and take some action. Tsdat currently includes two QC Error 
Handlers to allow the user to stop processing data and fail the pipeline or replace failed values with a specified 
_FillValue. Users are also able to specify their own custom Error Handlers to handle more complex cases as needed.
