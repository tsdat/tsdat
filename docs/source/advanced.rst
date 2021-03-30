.. advanced:

Advanced
########

Nearly all aspects of tsdat can be customized to suit individual use cases or project needs. For more information on 
how to customize various aspects of tsdat and to learn about more advanced recipies, consult the following sections:


Custom Pipelines
----------------

Coming soon...


Custom QC Operators
-------------------

Coming soon...


Custom QC Error Handlers
------------------------

Coming soon...


Custom Converters
-----------------

.. Converters are used to translate raw data into data used by the tsdat framework and made accessible to users through 
.. various user hooks. The default converter simply applies a units conversion to take the raw data with specified units
.. and convert it to the output units (I.e. can translate your input units of millimeters per second to output units of 
.. meters per second, if desired). 

.. Converters are defined in tsdat/io/converters.py and are associated with individual variables via the config file.

Coming soon...
