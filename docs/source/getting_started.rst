.. getting_started:

.. _examples folder: https://github.com/tsdat/tsdat/tree/master/examples
.. _pipeline template file:  https://github.com/tsdat/tsdat/blob/master/examples/templates/ingest_pipeline_template.yml
.. _Xarray: http://xarray.pydata.org/en/stable/
.. _netCDF: https://www.unidata.ucar.edu/software/netcdf/
.. _act-atmos: https://github.com/ARM-DOE/ACT

Getting Started
###############

Coming soon...

Installation
------------

.. Tsdat is not yet available on pypy. To install its dependencies so it can be run locally, we recommend using pip:
.. ``pip install -r requirements.txt``

Coming soon...


Using tsdat
-----------

Once tsdat is installed and you have defined a config file, you can run it on your input data using the following code::
    
    import tsdat
    config = tsdat.Config.load("path/to/yourconfigfile.yaml")
    storage = tsdat.storage.FilesystemStorage("path/to/storage/area")
    pipeline = tsdat.IngestPipeline(config, storage)
    pipeline.run("path/to/raw/file")

For detailed examples of how to set up and use tsdat, consult the tsdat `examples folder`_. These can also be used as 
a starting point for setting up particular use cases.

To learn how to set up a configuration file, consult this `pipeline template file`_.



Dependencies
------------

Tsdat is built using `Xarray`_ internals on top of the `netCDF`_ data format commonly used in Climate and 
Meteorological domains. Tsdat also makes use of the `act-atmos`_ for its quality tests and various utilities.
