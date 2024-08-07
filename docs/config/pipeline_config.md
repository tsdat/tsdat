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

## Adding a new pipeline

### Creating a Repository from the Pipeline Template

1. Use the Template:
    - Go to the [tsdat pipeline-template](https://github.com/tsdat/pipeline-template)repository on GitHub.
    - Click the "Use this template" button to create a new repository in your GitHub account based on this template.
    - If you need an older version, select "Include all branches" and set your desired branch as the default.
2. Clone the Repository:
    - Click the "Code" button on your new repository page to copy the repository URL.
    - Open a terminal and run:

      ```bash
      git clone <your-repository-url>
      ```

    - Navigate to the cloned repository:

      ```bash
      cd <your-repository-name>
      ```

### Setting Up Your Anaconda Environment

1. Open Terminal:
    - On Linux or Mac, open a regular terminal.
    - On Windows, open an Anaconda prompt or a WSL terminal if using Windows Subsystem for Linux.
2. Create and Activate Environment:
    - Run the following commands:

      ```bash
      conda env create --file=conda-environment.yaml
      conda activate tsdat-pipelines
      ```

3. Verify Environment:
    - Run tests to ensure the environment is set up correctly:

      ```bash
      pytest
      ```

    - If you encounter a pyproj warning, run:

      ```bash
      conda remove --force pyproj
      pip install pyproj
      ```

### Adding a New Pipeline

1. Generate New Pipeline Folder:
    - From the top-level repository folder, run:

      ```bash
      make cookies
      ```

      or

      ```bash
      cookiecutter templates/ingest -o ingest/
      ```

    - This command uses cookiecutter to generate a new pipeline folder inside the pipelines/ directory.
2. Follow Prompts:
    - Answer the prompts to customize your new pipeline. These prompts will guide you through setting up various aspects of the pipeline, such as naming and configuration.
3. Review and Customize:
    - Once cookiecutter completes, a new pipeline folder will appear inside pipelines/.
    - Open the README.md file in the new pipeline folder for further instructions on configuring, running, testing, and debugging your pipeline.
    - Customize the pipeline by updating configuration files, setting up input/output paths, and defining necessary data transformations or quality control measures.
