.. _transform_config:

Transformation / VAP Pipelines
==============================


Transformation pipelines, also referred to as Value-Added Product (VAP) pipelines, are tsdat pipelines
that use data from several standardized input sources and combine them in ways that add value to the
data.

.. warning::
    Tsdat support for transformation pipelines is currently in an alpha phase, meaning that new features
    are being actively developed and APIs involved may be relatively unstable as new use cases are added
    and requirements are discovered. We greatly appreciate any feedback on this new capability.


Tsdat transformation pipelines are configured in almost exactly the same way as the ingestion pipelines
you may already be used to. In fact, the tsdat **TransformationPipeline** class inherits all of its methods
and attributes from the **IngestPipeline** class and only overrides the retriever code to ensure that input
data are retrieved from the storage area.

Only the **pipeline.yaml** and **retriever.yaml** configuration files have any differences from their
counterparts for a tsdat ingest. These are shown below.




Pipeline Configuration File
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The pipeline configuration file for transformation pipelines is almost identical to its ingest pipeline
counterpart. There are only two differences:

* The **classname** should point to tsdat.TransformationPipeline, or a class derived from it.
* The **trigger** should be empty since transformation pipelines are currently run manually.

An example transformation pipeline ``pipeline.yaml`` file is shown below:

.. code-block:: yaml
    :emphasize-lines: 1,3

    classname: tsdat.TransformationPipeline

    triggers: {}

    retriever:
        path: pipelines/example_pipeline/config/retriever.yaml

    dataset:
        path: shared/config/dataset.yaml

    quality:
        path: shared/config/default-quality.yaml

    storage:
        path: shared/config/storage.yaml


Retriever Configuration File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The retriever configuration file for transformation pipelines is also similar to its ingest pipeline
counterpart, but there are some notable differences, mostly pertaining to how data from various input
sources should be combined. These are noted below:

* The **classname** should point to tsdat.StorageRetriever, or a class derived from it.
* If a **coord** (e.g., "time") does not have any shape-modifying **data_converters**, then its shape remains unchanged
* If a **data_var** does not have any shape-modifying converters then its shape must already match the shape of any coordinates that dimension it, or the pipeline will crash.
* The **NearestNeighbor** data converter was added to map data variables onto the correct coordinate grid.

``retriever.yaml``:

.. code-block:: yaml
    :emphasize-lines: 1,7,16,17,23,24

    classname: tsdat.StorageRetriever

    coords:
        time:
            .*buoy_z06\.a1.*:
                name: time
                data_converters: []

    data_vars:
        temperature:
            .*buoy_z07\.a1.*:
                name: temp
                data_converters:
                    - classname: tsdat.io.converters.UnitsConverter
                      input_units: degF
                    - classname: tsdat.io.converters.NearestNeighbor
                      coord: time

        humidity:
            .*buoy_z07\.a1.*:
                name: rh
                data_converters:
                    - classname: tsdat.io.converters.NearestNeighbor
                      coord: time
