.. _converters:


Converters
==========
Converters are classes that are used to convert units from the raw data to 
standardized format. Each Converter should extend the ``Converter`` base 
class. The Converter base class defines one method, **run**, which converts 
a numpy ndarray of variable data from the input units to the output units.  

Currently tsdat provides two converters for working with time data.  
``StringTimeConverter`` converts time values in a variety of string formats, 
and ``TimestampTimeConverter`` converts time values in long integer format.

In addition, tsdat provides a ``DefaultConverter`` which converts any units 
from one UDUNITS-2 supported units type to another. This converter will be 
initiated if a variable's output units (e.g "m") are specified differently 
from its input units (e.g. "mm") in the pipeline configuration file.

.. autosummary::
	:nosignatures:
	
	~tsdat.utils.converters.DefaultConverter
	~tsdat.utils.converters.StringTimeConverter
	~tsdat.utils.converters.TimestampTimeConverter

Converters are specified in the ``pipeline_config_<ingest_name>.yml`` file 
within variable definitions:

.. code-block:: yaml

  variables:
    time:
      input:
        name: time
        converter:
          classname: "tsdat.utils.converters.TimestampTimeConverter" # Converter name
          parameters:
            timezone: "US/Pacific"
            unit: "s"
      dims: [time]
      type: float
      attrs:
        long_name: Time (UTC) # automatically converts this without tz based on local computer
        standard_name: time
        units: "seconds since 1970-01-01T00:00:00"
		
    displacement:
      input:
        name: displacement
        units: "mm" # Units the input variable was measured in (DefaultConverter)
      dims:
        [dir, time] 
      type: float
      attrs:
        units: "m" # Units the variable should be output in (DefaultConverter)
        comment: "Translational motion as measured by the buoy"		
