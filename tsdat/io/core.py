import ntpath
from tsdat import TimeSeriesDataset, Config
from tsdat.io.file_handlers import NetCdfHandler, CsvHandler


# TODO: formats and handlers should be defined via setup tool hooks, not
# hardcoded here so that anyone can contribute a package with more readers/writers
class FileFormat:
    NETCDF = "netcdf"
    CSV = "csv"


handlers = {
    FileFormat.NETCDF: NetCdfHandler(),
    FileFormat.CSV: CsvHandler()
}


def save(dataset: TimeSeriesDataset, filename: str, file_format: str, **kwargs):
    """
    Save the given dataset to file
    :param dataset: The dataset to save
    :param filename: An absolute or relative path to the file including filename
    :param file_format: Use FileFormat to find the supported formats
    """
    # Overwrite the datastream name to match the new filename
    dataset.set_datastream_name(ntpath.basename(filename))
    handlers[file_format].write(dataset, filename, **kwargs)


def load(filename: str, file_format: str, config: Config = None, **kwargs):
    """
    Save the given dataset to file
    :param filename: An absolute or relative path to the file including filename
    :param file_format: Use FileFormat to find the supported formats
    :param config: external metadata to associate with this dataset (can use if the
    file loaded did not include any metadata)
    :return:
    :rtype: TimeSeriesDataset
    """
    dataset: TimeSeriesDataset = handlers[file_format].read(filename, config, **kwargs)
    dataset.set_datastream_name(ntpath.basename(filename))
    return dataset

