.. _pipeline_config:

Pipeline Configuration
======================
The pipeline config file ``pipeline_config.yml`` is used to define how the pipeline will standardize input data.
It defines all the pieces of your standardized dataset, as described in the in the
`Data Standards Document <https://github.com/tsdat/data_standards/blob/main/ME_DataStandards.pdf>`_.
Specifically, it identifies the following components:

#. **Global attributes** - dataset metadata
#. **Dimensions** - shape of data
#. **Coordinate variables** - coordinate values for a specific dimension
#. **Data variables** - all other variables in the dataset
#. **Quality management** - quality tests to be performed for each variable and any associated corrections to be applied for failing tests.

Each pipeline template will include a starter pipeline config file in the config folder.
It will work out of the box, but the configuration should be tweaked according to the
specifics of your dataset.

A full annotated example of an ingest pipeline config file is provided below and 
can also be referenced in the
`Tsdat github repository <https://github.com/tsdat/tsdat/blob/master/examples/templates/ingest_pipeline_template.yml>`_
 
.. literalinclude:: ../figures/ingest_pipeline_template.yml
    :linenos:
    :language: yaml
	