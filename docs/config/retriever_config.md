# Retriever Configuration

The retriever config file `retriever.yaml` describes how to read raw input data. Specifically, it identifies the
following components:

1. **Readers** - the classes, indexed by file name pattern, used to read specific input file types
2. **Coordinate variables** - where to find coordinate variables in the raw dataset(s).
3. **Data variables** - where to find all other variables in the raw dataset(s).

In the config file, each variable is indexed by its corresponding name in the output dataset.yaml file. For each output
variable, you can specify the corresponding name in the input data, indexed by input file name pattern (more than one
pattern may be needed for cases where the variable can be read from different input files). In addition to specifying
the name of the variable in the input file, you also can define one or more data converters that can be used to convert
the value from raw data format to the format required by the output dataset. For example, use the `StringToDatetime`
converter to convert string time values into a `numpy.datetime64` array.

Each pipeline template will include a starter retriever config file in the config folder. It will work out of the box,
but the configuration should be tweaked according to the specifics of your data. Consult the
[getting started](../getting_started.md) section for more information on getting started with a template.

!!! note

    For almost all cases, defining custom DataReader will be enough to handle custom input files. However in rare edge
    cases, a custom retriever may be necessary. To implement a custom retriever, you can extend the
    `tsdat.io.base.Retriever` class.

## Readers

Readers declare the classes that should be used to read raw input files. If you define a custom reader class, it can
contain any level of pre-analysis that the user desires; the only requirement is that it returns an xarray Dataset.
Readers must extend the abstract class `tsdat.DataReader` and should implement the `read()` method like so:

```python
def read(self, input_key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
    """Uses the input key to open a resource and load data as a xr.Dataset object or as
    a mapping of strings to xr.Dataset objects.

    In most cases DataReaders will only need to return a single xr.Dataset, but
    occasionally some types of inputs necessitate that the data loaded from the
    input_key be returned as a mapping. For example, if the input_key is a path to a
    zip file containing multiple disparate datasets, then returning a mapping is
    appropriate.

    Args:
        input_key (str): An input key matching the DataReader's regex pattern that
        should be used to load data.

    Returns:
        Union[xr.Dataset, Dict[str, xr.Dataset]]: The raw data extracted from the
        provided input key.
    """
```

To specify a custom reader, in the readers section of the `retriever.yaml` file, use a Python regular expression to
match any specific file name pattern that should be read by that reader. For example, this configuration would use the
`CSVReader` to read all files input to the pipeline:

```yaml
.*:
  classname: tsdat.io.readers.CSVReader
  parameters:
    read_csv:
      # This treats the first column (time) as a data variable, which helps for when
      # the pandas DataFrame is converted to an xarray Dataset
      index_col: false

      # This tells pandas to look at row index 2 (0-based) for the header
      header: 2
```

## Converters

Converters are used to convert a variable's data from one format to another. In the retriever config file, you can
specify one or more converters per variable. If more than one converter is defined, they will be invoked sequentially.
Each converter needs to extend the `tsdat.DataConverter` abstract class. It must implement the `convert()` method as
shown below:

```python
def convert(
    self,
    data: xr.DataArray,
    variable_name: str,
    dataset_config: DatasetConfig,
    retrieved_dataset: RetrievedDataset,
    **kwargs: Any,
) -> Optional[xr.DataArray]:
    """Runs the data converter on the retrieved data.

    Args:
        data (xr.DataArray): The retrieved DataArray to convert.
        retrieved_dataset (RetrievedDataset): The retrieved dataset containing data
            to convert.
        dataset_config (DatasetConfig): The output dataset configuration.
        variable_name (str): The name of the variable to convert.

    Returns:
        xr.DataArray: The converted DataArray for the specified variable.
    """
```

## Coordinates

The coordinates section in the `retriever.yaml` file specifies the coordinate variables that are used to read and interpret raw input data. This section defines the dimensions and attributes of the coordinates, such as time or spatial coordinates, which are essential for accurately mapping the data within the dataset. The configuration ensures that the data is properly aligned and can be processed effectively by the tsdat framework

```yaml
coords:
  # Specify the coords that should be retrieved from any inputs
  time:  # Coordinate variable for time
    # Mapping of regex pattern (matching input key/file) to input name & converter(s) to
    # run. The default is .*, which matches everything. Put the most specific patterns
    # first because searching happens top -> down and stops at the first match.
    .*:  # Regex pattern to match all input keys/files
      # The name of the input variable as returned by the selected reader. If using a
      # built-in DataReader like the CSVReader or NetCDFReader, then will be exactly the
      # same as the name of the variable in the input file.
      name: Timestamp (end of interval)  # Name of the input variable in the raw data

      # Optionally specify converters to run. The one below converts string values into
      # datetime64 objects. It requests two arguments: format and timezone. Format is
      # the string time format of the input data (see strftime.org for more info), and
      # timezone is the timezone of the input measurement.
      data_converters:  # List of converters to apply to the input data
        - classname: tsdat.io.converters.StringToDatetime  # Converter class to use
          format: "%Y-%m-%d %H:%M:%S"  # Format of the time string in the input data
          timezone: UTC  # Timezone of the input data
```

## Data Variables

`Data_vars` are used to specify the data variables that should be retrieved from the input data sources. Similar to the coords section, `data_vars` define how to read and preprocess variables from the raw input data, including the variable name, its source in the input data, and any data converters to be applied. Each `data_var` entry typically includes information on how to handle the data, such as unit conversions or other preprocessing steps, and uses regex patterns to match input sources.

```yaml
data_vars:
  example_var:  # Data variable name to be used in the processed dataset
    .*:  # Regex pattern to match all input keys/files
      name: Example  # Name of the input variable in the raw data

      # Optionally specify converters to run. The one below converts units of the input
      # data. It requests the input_units argument, which specifies the units of the
      # input measurement.
      data_converters:  # List of converters to apply to the input data
        - classname: tsdat.io.converters.UnitsConverter  # Converter class to use
          input_units: km  # Units of the input data to be converted
```
