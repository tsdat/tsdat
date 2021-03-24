.. filehandlers:

FileHandlers
############

FileHandlers provide a customizable interface between files entering and leaving the tsdat framework, which allows the
framework to be file-format-agnostic on both its input and its output. FileHandlers can be registered with specific
file type(s) or to match a specified regular expression on the filename. The only requirement for FileHandlers is that 
they implement read and write methods. A simple FileHandler for netCDF files is defined below::

    import tsdat
    import xarray as xr

    @tsdat.register_filehandler(ext="nc")
    class NetCdfFileHandler():
        def read(filename: str, *args, **kwargs) -> xr.Dataset:
            return xr.open_dataset(filename, **kwargs)
        
        def write(filename: str, dataset: xr.Dataset, *args, **kwargs):
            dataset.to_netcdf(filename, **kwargs)


.. ME data pipelines will include support for writing netCDF, CSV, and Parquet files out-of-the-box, but the data 
.. pipelines are file-format-agnostic and support for other formats can be added through a lightweight plugin 
.. architecture.  It is highly recommended that ME data pipeline users use netCDF to store processed data files, as it is
.. a self-documenting format that is sufficient to store all data and metadata over data of any dimensionality.  Many file
.. formats such as CSV and Parquet do not support embedding dataset and variable metadata.  To prevent the loss of 
.. metadata, plugins will be are strongly encouraged to write all metadata to an accompanying YAML file if the file format
.. does not natively support embedded metadata.
