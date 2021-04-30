.. getting_started:

.. _examples folder: https://github.com/tsdat/tsdat/tree/master/examples
.. _pipeline template file:  https://github.com/tsdat/tsdat/blob/master/examples/templates/ingest_pipeline_template.yml
.. _Xarray: http://xarray.pydata.org/en/stable/
.. _netCDF: https://www.unidata.ucar.edu/software/netcdf/
.. _act-atmos: https://github.com/ARM-DOE/ACT
.. _anaconda: https://www.anaconda.com
.. _docker: https://www.docker.com


.. _getting-started:

Getting Started
###############

.. _installation:

Installation
************

You can install tsdat simply by running ``pip install tsdat`` in a console 
window, but that is probably not what you want, unless you are looking to build
the architecture around a data pipeline from scratch. 

If you want to start with an existing codebase (which is what we recommend for 
most people), you should start by cloning a template repository on GitHub. You 
can find a list of template repositories for tsdat at 
https://github.com/tsdat/template-repositories

Each template repository will come with specific instructions on how to install
the relevant packages for that template repository, but in general it should be
as simple as:

#.	Click the “Use this template” button to create your own repository.

#.	Run ``pip install tsdat`` in a terminal window.

#.	Run ``python3 run_pipeline.py`` in a terminal window at the top level of your repository. By default, the repository template will come configured to run on an example dataset.

#.	Modify the configuration files, code hooks, and add your own dataset. See the customization section of the documentation for more information on how this can be done.


We recommend using an `anaconda`_ environment or (preferably) a `docker`_ 
container to manage your project’s environment if you plan on deploying this
project to AWS or a production system in the future.

.. _using-tsdat:

Using tsdat
***********

Once tsdat is installed and you have defined a config file, you can run it on 
your input data using the following code:

.. code-block:: python

    import tsdat
    config = tsdat.Config.load("path/to/yourconfigfile.yaml")
    storage = tsdat.storage.FilesystemStorage("path/to/storage/area")
    pipeline = tsdat.IngestPipeline(config, storage)
    pipeline.run("path/to/raw/file")


For detailed examples of how to set up and use tsdat, consult the 
:ref:`examples-and-tutorials` section.
