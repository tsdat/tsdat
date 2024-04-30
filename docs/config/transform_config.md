# Transformation / VAP Pipelines

Transformation pipelines, also referred to as Value-Added Product (VAP) pipelines, are tsdat pipelines that use data
from several standardized input sources and combine them in ways that add value to the data.

!!! warning

    Tsdat support for transformation pipelines is currently in a beta phase, meaning that new features are being
    actively developed and APIs involved may be relatively unstable as new use cases are added and requirements are
    discovered. We greatly appreciate any feedback on this new capability.

## Installation

One additional library is needed in order to use the new transformation methods:
[adi_py](https://anaconda.org/arm-doe/adi_py).

This can be installed via `conda install -c arm-doe adi_py>=3.19.2`.

This library contains C code and cython bindings from the Atmospheric Radiation Measurement (ARM) Program's ARM Data
Integrator (ADI) transformation library, which provides one critical feature over the transformation methods built-in to
xarray: handling data quality as part of the transformation process. The `adi_py` library reads data quality flags from
qc variables and will opt to not use points flagged as bad in transformations such as interpolation and averaging.
Additionally, the library outputs a qc variable for each transformed variable describing the quality of the
transformation based on the quality of any input qc flags found in the input data.

## Configuration

Tsdat transformation pipelines are configured in almost exactly the same way as the ingestion pipelines you may already
be used to. In fact, the `tsdat.TransformationPipeline` class inherits all of its methods and attributes from the
`tsdat.IngestPipeline` class and only overrides the retriever code to ensure that input data are retrieved from the
storage area.

Only the `pipeline.yaml` and `retriever.yaml` configuration files have any differences from their counterparts for a
tsdat ingest. These are shown in the sections below.

### Pipeline Configuration File

The pipeline configuration file for transformation pipelines is almost identical to its ingest pipeline counterpart.
There are only a few differences:

- The `classname` should point to `tsdat.TransformationPipeline`, or a class derived from it.
- The `parameters` for the class should include a `datastreams` entry mapping to a list of input datastreams that are
    needed as input to the pipeline.
- The `trigger` should be empty since transformation pipelines are currently run manually.

An example transformation pipeline `pipeline.yaml` file is shown below. Highlighted lines show notable differences from
a typical pipeline configuration file for an `IngestPipeline`.

```yaml hl_lines="1 2 3 4 5 7"
classname: tsdat.TransformationPipeline
parameters: 
    datastreams:
        - humboldt.lidar.b0
        - humboldt.met.b0

triggers: []

retriever:
    path: pipelines/example_pipeline/config/retriever.yaml

dataset:
    path: pipelines/example_pipeline/config/dataset.yaml

quality:
    path: shared/config/default-quality.yaml

storage:
    path: shared/config/storage.yaml
```

### Retriever Configuration File

The retriever configuration file for transformation pipelines is also similar to its ingest pipeline counterpart, but
there are some notable differences, mostly pertaining to how data from various input sources should be combined. These
are noted below:

- The `classname` should point to `tsdat.StorageRetriever`, or a class derived from it. This class requires additional
    `transformation_parameters` to be specified.
- The `tsdat.transform` module was added, including methods for creating time coordinate grids and various
    transformation methods: `NearestNeighbor`, `BinAverage`, `Interpolate`, or `Automatic`.

An example `retriever.yaml` file is shown below. Highlighted lines show notable differences from a typical retriever
configuration file for an `IngestPipeline`.

```yaml hl_lines="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 23 24 25 26 35 36 42 43"
classname: tsdat.StorageRetriever
parameters:
  # Set coordinate system defaults for alignment, range, width
  transformation_parameters:
    # Alignment is one of LEFT, RIGHT, CENTER and indicates where the output point should
    # lie in relation to the reported timestamp range.
    alignment:
      time: CENTER
    # Range is how far to look for the previous/next points when transforming over an
    # interval. E.g., for nearest neighbor, this is how far to look for the closest available
    # point. If nothing is close enough, the output corresponding with that timestamp will be
    # NaN/missing
    range:
      time: 900s
    # Width is the size of the output dimension bins. E.g. width=300s with center alignment
    # would mean that each timestamp in the output represents the period from 150s before and 150s
    # after the reported timestamp.
    width:
      time: 300s

coords:
  time:
    name: NA  # not retrieved from input; this will be autogenerated instead
    data_converters:
      - classname: tsdat.transform.CreateTimeGrid
        interval: 5min

data_vars:
  temperature:
    .*met\.b0.*:
      name: temp
      data_converters:
        - classname: tsdat.io.converters.UnitsConverter
          input_units: degF
        - classname: tsdat.transform.NearestNeighbor
          coord: time

  humidity:
    .*met\.b0.*:
      name: rh
      data_converters:
        - classname: tsdat.transform.NearestNeighbor
          coord: time
```

### Pipeline Code Hooks

The `TransformationPipeline` class provides one additional hook that is not available in the `IngestPipeline` class: the
`hook_customize_input_datasets` hook. This function allows you to customize input datasets/files before they are merged
onto the same coordinate grid.

```python
def hook_customize_input_datasets(
    self, input_datasets: Dict[str, xr.Dataset], **kwargs: Any
) -> Dict[str, xr.Dataset]:
    """Code hook to customize any input datasets prior to datastreams being combined
    and data converters being run.

    Args:
        input_datasets (Dict[str, xr.Dataset]): The dictionary of input key (str) to
            input dataset. Note that for transformation pipelines, input keys !=
            input filename, rather each input key is a combination of the datastream
            and date range used to pull the input data from the storage retriever.

    Returns:
        Dict[str, xr.Dataset]: The customized input datasets.
    """
    return input_datasets
```
