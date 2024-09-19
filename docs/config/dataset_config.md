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

## Sample Configuration of Dataset.yaml

This sample configuration illustrates the structure of a `dataset.yaml` file used in Tsdat, outlining essential components such as metadata attributes, coordinate definitions, and data variable specifications. It serves as a template for defining time-series datasets, divided into three main sections: 'attrs', 'coords', and 'data_vars'. Annotations throughout the sample explain each section's purpose and field meanings, facilitating easy adaptation to specific data requirements.

```yaml
# The 'attrs' section defines metadata attributes for the dataset
attrs:
  title: test  # The title of the dataset
  description: test_description  # A brief description of the dataset
  location_id: test_location  # Identifier for the location of the data
  dataset_name: test  # Name of the dataset
  data_level: a1  # Data processing level (e.g., raw, quality controlled, etc.)
  # qualifier:  # Optional: Additional qualifier for the dataset
  # temporal:  # Optional: Temporal information about the dataset
  # institution:  # Optional: Institution responsible for the dataset

# The 'coords' section defines the coordinate variables
coords:
  time:  # Time coordinate
    dims: [time]  # Dimension of the time coordinate
    dtype: datetime64[s]  # Data type for time (datetime with second precision)
    attrs:
      units: Seconds since 1970-01-01 00:00:00  # Units for the time coordinate

# The 'data_vars' section defines the data variables in the dataset
data_vars:
  example_var:  # Name of the example variable
    dims: [time]  # Dimension of the variable (using the time coordinate)
    dtype: float  # Data type of the variable
    attrs:
      long_name: Example Variable  # Full descriptive name of the variable
      units: km  # Units of measurement for the variable
```

## Units

Units are an important (and required) metadata attribute on coordinates and data variables. Tsdat strongly recommends
using units pulled from the [UDUNITS2](https://ncics.org/portfolio/other-resources/udunits2/) database. If the units are
not known or if a particular variable is unitless, then `units: "1"` may be used.
