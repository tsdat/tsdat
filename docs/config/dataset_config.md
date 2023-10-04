# Dataset Configuration

The dataset config file `dataset.yaml` defines the format of your standardized dataset, as described in the in the
[Data Standards Document](https://github.com/tsdat/data_standards/blob/main/ME_DataStandards.pdf). Specifically, it
identifies the following components:

1. **Global attributes** - dataset metadata
2. **Dimensions** - shape of data
3. **Coordinate variables** - coordinate values for a specific dimension
4. **Data variables** - all other variables in the dataset

Each pipeline template will include a starter dataset config file in the config folder. It will work out of the box, but
the configuration should be tweaked according to the specifics of your dataset. Consult the
[getting-started](../getting_started.md) section for more information on getting started with a template.

!!! note

    Tsdat templates come complete with a VS Code IDE configuration that will provide inline documentation and
    auto-completions for your yaml configuration files. Consult the [tutorials](./pipeline_config.md) section for more
    information on editing your pipeline in VS Code.
