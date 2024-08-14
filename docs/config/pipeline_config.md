# Pipeline Configuration

The pipeline config file `pipeline.yaml` describes the configuration of your pipeline:

1. **Triggers** - which file input file patterns should trigger the pipeline
2. **Pipeline Class** - dotted class name of the pipeline to use
3. **Config Files** - the `retriever`, `dataset`, `quality`, and `storage` config files and overrides to use

Each pipeline template will include a starter pipeline config file in the config folder. It will work out of the box,
but the configuration should be tweaked according to the specifics of your pipeline. Consult the
[getting started](../getting_started.md) section for more information on getting started with a template.

!!!
  To prevent redundancy, Tsdat config files are designed to be shared across multiple pipelines. In the pipeline config
  file, you can specify a shared config file to use (ie., `shared/config/dataset.yaml`) and then override specific
  values in the overrides section.

An annotated example of an ingest pipeline config file is provided below:

```yaml
--8<-- "docs/figures/pipeline.yaml"
```

## Overrides

You may have noticed the **overrides** option used in the dataset configuration. This option can be used to override or
add values in the source configuration file. Here we are changing the `location_id` global attribute to `"sgp"` and
adding a new attribute to the data variable named `"first"`. Overrides enhance the reusability of configuration files,
allowing you to define a base configuration file and override specific features of it as needed for instruments at
different sites.

Consider the following example:

```yaml title="pipelines/lidar/config/dataset.yaml"
attrs:
  title: My Dataset
  location_id: sgp
  dataset_name: lidar
  data_level: b1
coords:
  time:
    dims: [time]
    dtype: datetime64[ns]
    attrs:
      units: Seconds since 1970-01-01 00:00:00
data_vars:
  wind_speed:
    dims: [time]
    dtype: float
    attrs:
      units: m/s
      valid_range: [0, 30]
```

```yaml title="pipelines/lidar/config/pipeline.yaml"
# ... 

dataset:
  path: pipelines/lidar/config/dataset.yaml
  overrides:

    # Changing existing properties via dictionary access
    /attrs/location_id: hou

    # Adding properties / attributes via dictionary access
    /data_vars/wind_speed/attrs/comment: This adds a 'comment' attribute!

    # Adding new variables
    /data_vars/wind_dir:
      dims: [time]
      dtype: float
      attrs:
        units: deg
        comment: This is a brand new variable called 'wind_dir'

    # Changing properties by array index
    /data_variables/wind_speed/attrs/valid_range/1: 50
  
  # ...
```

This is equivalent to defining an entirely new `dataset.yaml` file like below, but with the version above we only need
to change a few lines:

```yaml title="duplicate pipelines/lidar/config/dataset.yaml (don't do this)"
attrs:
  title: My Dataset
  location_id: hou
  dataset_name: lidar
  data_level: b1
coords:
  time:
    dims: [time]
    dtype: datetime64[ns]
    attrs:
      units: Seconds since 1970-01-01 00:00:00
data_vars:
  wind_speed:
    dims: [time]
    dtype: float
    attrs:
      units: m/s
      valid_range: [0, 50]
      comment: This adds a 'comment' attribute!
  wind_dir:
    dims: [time]
    dtype: float
    attrs:
      units: deg
      comment: This is a brand new variable called 'wind_dir'
```

## Adding a New Pipeline

### Creating a New `pipeline.yaml` File

When working with an existing ingest, you might need to create a new `pipeline.yaml` file to accommodate different configurations. For example, you may want to set up pipelines for different sites, apply different metadata, or handle other specific processing needs. Adding a new `pipeline.yaml` file allows you to maintain organization and flexibility within your project.

#### Reasons for Adding a New `pipeline.yaml` File

- **Different Site Configurations:** If you're processing data from multiple sites, each site may have unique settings or parameters. Creating separate `pipeline.yaml` files for each site ensures that these configurations are handled properly.
- **Varying Metadata:** Different datasets might require distinct metadata configurations. Separate `pipeline.yaml` files can help manage these variations efficiently.
- **Specialized Processing:** You might need different processing steps or quality control measures for different datasets. Multiple `pipeline.yaml` files allow you to customize these processes without disrupting the overall pipeline structure.

#### Suggested Naming Conventions

To keep your project organized, consider adopting a clear and consistent naming convention for your `pipeline.yaml` files. Here are some suggestions:

Site-Specific Pipelines:

- `pipeline_sgp.yaml` for the Southern Great Plains site.
- `pipeline_nsa.yaml` for the North Slope of Alaska site.

Metadata Variations:

- `pipeline_metadata_v1.yaml` for the first version of metadata.
- `pipeline_metadata_alt.yaml` for an alternative metadata configuration.

Specialized Processing:

- `pipeline_qc.yaml` for a pipeline focused on quality control.
- `pipeline_transform.yaml` for pipelines that require specific data transformations.

#### Example: Adding a New `pipeline.yaml` File

Let's walk through an example of how to add a new `pipeline.yaml` file:

1. **Duplicate an Existing `pipeline.yaml` File:**

    Navigate to the `pipelines/` directory in your repository.

    Copy an existing `pipeline.yaml` file that is closest to what you need:

    ```bash
    cp pipelines/pipeline_sgp.yaml pipelines/pipeline_nsa.yaml
    ```

2. **Customize the New pipeline.yaml File:**

    Open the newly created pipeline_nsa.yaml file in your text editor.

    Update the configurations, such as site name, paths, metadata, and any specific processing steps required for this pipeline.

3. **Integrate the New Pipeline:**

    Ensure that your project references the new pipeline.yaml file correctly.

    Update any scripts or configurations that need to utilize the new pipeline.

4. **Test the New Pipeline:**

    Run tests to verify that the new pipeline functions as expected. This might include running the pipeline on sample data and checking the output against expected results.
