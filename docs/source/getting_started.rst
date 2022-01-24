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

To get started developing a tsdat pipeline, we suggest following the following
steps, which are explained in more detail in the linked sections:

1. :ref:`Install tsdat<Installation>`
2. :ref:`Get a template<template>`
3. :ref:`Configure template<configuring_tsdat>`
4. :ref:`Run pipeline<running-tsdat>`


.. _prerequisites:

Prerequisites
*************
Tsdat requires `Python 3.8+ <https://www.python.org/downloads/>`_


.. _installation:

Installation
************
You can install tsdat simply by running::

    pip install tsdat
  
in a console window.  


.. _template:

Developing a Tsdat Pipeline
***************************
The recommended way to set up a Tsdat pipeline is to use a GitHub repository template.
You can find a list of template repositories for tsdat at `<https://github.com/tsdat/template-repositories>`_.

#. `Ingest Template <https://github.com/tsdat/ingest-template>`_

    Use this template to run ingest pipelines on your local computer.

#. `AWS Ingest Template <https://github.com/tsdat/ingest-template-aws>`_

    Use this template to run ingest pipelines on AWS.  (It requires an AWS account.)

Once you have selected the template to use, select the "Use this template" button
to create a new repository at your specified location with the template contents.

.. figure:: figures/use_template.png
   :alt: Use a GitHub pipeline repository template to jumpstart tsdat development.

Once you have created a new repository from the template, you can clone your 
repository to your local desktop and start developing. By default, the repository
template will come pre-configured to run out-of-the-box on an example dataset.

See the :ref:`pipeline template tutorial<examples_and_tutorials>` walkthroughs for how 
to set up each of these templates.

See :ref:`configuring your pipeline<configuring_tsdat>` for more information on 
tsdat-specific configuration file and code customizations.  In addition, make
sure to read the **README.md** file associated with your template for any
template-specific instructions.


.. _running-tsdat:

Running Your Tsdat Pipeline
****************************

Once tsdat is installed and your pipeline template is configured, you can run it locally on 
your input data using the following code from a terminal window at the top level of your repository:

#. `Ingest Template <https://github.com/tsdat/ingest-template>`_::

    python ingest/<ingest-name>/runner.py

#. `AWS Ingest Template <https://github.com/tsdat/ingest-template-aws>`_::

    python tests/test_pipeline.py

For detailed examples of how to set up and use tsdat, consult the 
:ref:`examples_and_tutorials` section.
