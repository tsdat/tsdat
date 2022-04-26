.. _pipeline_config:

Pipeline Configuration
========================
The pipeline config file ``pipeline.yaml`` describes the configuration of your pipeline:

#. **Triggers** - which file input file patterns should trigger this pipeline
#. **Ingest Class** - class name of the ingest pipeline to use
#. **Dependent Config Files** - which yaml files to use for the retriever, dataset, quality management, and storage

Each pipeline template will include a starter pipeline config file in the config folder.
It will work out of the box, but the configuration should be tweaked according to the
specifics of your pipeline.  Consult the :ref:`getting-started` section for more information on getting started with a template.

.. note::
   To prevent redundancy, Tsdat config files are designed to be shared across multiple pipelines.  In the pipeline
   config file, you can specify a shared config file to use (ie., shared/config/dataset.yaml) and then override
   specific values in the overrides section.

An annotated example of an ingest pipeline config file is provided below:
 
.. literalinclude:: ../figures/pipeline.yaml
    :linenos:
    :language: yaml
	