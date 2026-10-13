# Storage Configuration

The storage config file `storage.yaml` describes how the output dataset will be saved to persistent storage.
Specifically, it identifies the following two components:

1. **Storage Class** - the class that will be used to persist output dataset
2. **Handler** - the class that will be used to write datasets and read datasets back from the store

Each pipeline template will include a starter storage config file in the config folder. It will work out of the box, but
the configuration should be tweaked according to the specifics of your pipeline. Consult the
[getting started](../getting_started.md) section for more information on getting started with a template.

## Storage Classes

Currently there are three storage classes provided out of the box:

1. `tsdat.FileSystem` - saves to local filesystem
2. `tsdat.FileSystemS3` - saves to an AWS S3 bucket (requires an AWS account)
3. `tsdat.ZarrLocalStorage` - saves to local filesystem in a zarr format.

These are all file-based storage classes. For the `FileSystem` and `FileSystemS3` classes, users can specify the output
file format via the `handler` classname parameter of the storage config file, although the default `NetCDFHandler` is
recommended for most applications. For `ZarrLocalStorage` the default is `ZarrHandler` and should not be changed.
Directory-based Zarr storage is not supported by `FileSystemS3`; use `ZarrLocalStorage` for local Zarr archives.

Each of these file-based storage classes allow configuration of where output files should be saved. This includes both
ancillary files (such as plots, reference files that may be created during processing, etc) and the data files produced
via processing:

```yaml title="storage.yaml"
classname: tsdat.FileSystem
parameters:
    storage_root: storage/root

    # The directory structure under storage_root where ancillary/data files are saved.
    # Allows substitution of the following parameters using curly braces '{}':
    # 
    # * ``extension``: the file extension (e.g., 'png', 'nc').
    # * ``datastream`` from the related xr.Dataset object's global attributes.
    # * ``location_id`` from the related xr.Dataset object's global attributes.
    # * ``data_level`` from the related xr.Dataset object's global attributes.
    # * ``year, month, day, hour, minute, second`` of the first timestamp in the data.
    # * ``date_time``: the first timestamp in the file formatted as "YYYYMMDD.hhmmss".
    # * The names of any other global attributes of the related xr.Dataset object.
    ancillary_storage_path: ancillary/{location_id}/{datastream}
    data_storage_path: data/{location_id}/{datastream}

    # Template string to use for ancillary/data filenames
    # Allows substitution of the following parameters using curly braces '{}':
    # 
    # * ``title``: a provided label for the ancillary file or plot.
    # * ``extension``: the file extension (e.g., 'png', 'nc').
    # * ``datastream`` from the related xr.Dataset object's global attributes.
    # * ``location_id`` from the related xr.Dataset object's global attributes.
    # * ``data_level`` from the related xr.Dataset object's global attributes.
    # * ``date_time``: the first timestamp in the file formatted as "YYYYMMDD.hhmmss".
    # * The names of any other global attributes of the related xr.Dataset object.
    # At a minimum the template must include ``{date_time}``.
    ancillary_filename_template: "{datastream}.{date_time}.{title}.{extension}"
    data_filename_template: "{datastream}.{date_time}.{extension}"

handler:
    classname: tsdat.NetCDFHandler
```

### Fetching a time range

`FileSystem` and `FileSystemS3` read files whose filename timestamps fall in the requested inclusive range, plus the
immediately preceding file (or files sharing its timestamp). They then select samples in the requested range. This
handles a file spanning the request start without opening every historical file. Because filenames record start times
only, an older file that spans the request but starts before a newer predecessor will not be found. Keep file spans
short or provide a custom storage implementation with an end-time index when arbitrary overlaps are required.

### S3 bucket setup

`FileSystemS3` checks for an existing bucket during initialization. Bucket creation is disabled by default: missing
buckets raise an error rather than being created as a side effect. Set `parameters.create_bucket: true` to create a
missing bucket explicitly (requires AWS permissions). Access-denied and other AWS errors are not treated as missing
buckets. Buckets created outside `us-east-1` use the configured `region` as their location constraint.

Ancillary files made with `storage.uploadable_dir()` are uploaded on normal exit and the temporary directory is removed
even if plotting or uploading raises an exception.

!!! note
    To implement custom storage, such as storing in a database, you must extend the `tsdat.Storage` base class.

## Handler Classes

Handlers declare the class that should be used to write output datasets and to read datasets back from persistent
storage. The `NetCDFHandler` is the default handler, but you can add a custom handler to add additional file formats or
to write to a different storage medium such as a database. The only requirement is that it can read and write to and
from an Xarray dataset. Handlers must extend the `tsdat.DataHandler` base class and encapsulate a `DataReader` and
`DataWriter` class, which should implement the following two methods, respectively:

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

def write(self, dataset: xr.Dataset, **kwargs: Any) -> None:
    """Writes the dataset to the storage area. This method is typically called by
    the tsdat storage API, which will be responsible for providing any additional
    parameters required by subclasses of the tsdat.io.base.DataWriter class.

    Args:
        dataset (xr.Dataset): The dataset to save.
    """
```
