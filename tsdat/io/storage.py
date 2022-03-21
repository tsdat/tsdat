import abc
import os
import re
import shutil
import tempfile
import xarray as xr
import yaml
from datetime import datetime
from typing import List, Union, Any, Dict

from tsdat.config.utils import instantiate_handler, configure_yaml
from tsdat.io import FileHandler
from tsdat.utils import DSUtil


def _is_image(x):
    return True if DSUtil.is_image(x.__str__()) else False


def _is_raw(x):
    return True if ".raw." in x.__str__() else False


class DatastreamStorage(abc.ABC):
    """DatastreamStorage is the base class for providing
    access to processed data files in a persistent archive.
    DatastreamStorage provides shortcut methods to find
    files based upon date, datastream name, file type,
    etc.  This is the class that should be used to save
    and retrieve processed data files.  Use the
    DatastreamStorage.from_config() method to construct the
    appropriate subclass instance based upon a storage config
    file.
    """

    default_file_type = None

    # Stores the map of file types to filter functions that will
    # be loaded from the storage config file and is used to
    # find specific files in the store.
    file_filters = {"plots": _is_image, "raw": _is_raw}

    # Stores the map of supported file exensions that will be loaded
    # from the storage config file.
    output_file_extensions = {}

    @staticmethod
    def from_config(storage_config_file: str):
        """Load a yaml config file which provides the storage constructor
        parameters.

        :param storage_config_file: The path to the config file to load
        :type storage_config_file: str
        :return: A subclass instance created from the config file.
        :rtype: DatastreamStorage
        """
        # Add the config folder as a special environment parameter before we
        # load the yaml file
        config_folder = os.path.dirname(storage_config_file)
        os.environ["CONFIG_DIR"] = config_folder

        # Configure yaml to substitute environment variables
        configure_yaml()

        # Load the config file
        with open(storage_config_file, "r") as file:
            storage_dict = yaml.load(file, Loader=yaml.SafeLoader).get("storage", {})

        # Now instantiate the storage
        storage = instantiate_handler(handler_desc=storage_dict)

        # Now we need to register all the file handlers
        # First do the inputs
        input_handlers = storage_dict.get("file_handlers", {}).get("input", {})
        for handler_dict in input_handlers.values():
            handler = instantiate_handler(handler_desc=handler_dict)
            FileHandler.register_file_handler(
                "read", handler_dict["file_pattern"], handler
            )

        # Now the outputs
        output_handlers = storage_dict.get("file_handlers", {}).get("output", {})
        for key, handler_dict in output_handlers.items():
            file_extension = handler_dict["file_extension"]
            file_pattern = f".*\\{file_extension}"

            # First register the writers
            handler = instantiate_handler(handler_desc=handler_dict)
            FileHandler.register_file_handler("write", file_pattern, handler)

            # Now register the file patterns for finding files in the store
            regex = re.compile(file_pattern)

            def filter_func(x):
                return True if regex.match(x.__str__()) else False

            DatastreamStorage.file_filters[key] = filter_func
            DatastreamStorage.output_file_extensions[key] = file_extension

            if DatastreamStorage.default_file_type is None:
                # Use the first output type registered as the default type
                DatastreamStorage.default_file_type = key

        return storage

    def __init__(self, parameters: Union[Dict, None] = None):
        parameters = parameters if parameters is not None else dict()
        self.parameters = parameters
        retain_input_files = parameters.get("retain_input_files", "False")
        if type(retain_input_files) == str:
            retain_input_files = (
                False if retain_input_files.lower() == "false" else True
            )

        self.remove_input_files = not retain_input_files

    @property
    def tmp(self):
        """Each subclass should define the tmp property, which provides
        access to a TemporaryStorage object that is used to efficiently
        handle reading/writing temporary files used during the processing
        pipeline, or to perform fileystem actions on files other than
        processed datastream files that reside in the same filesystem as
        the DatastreamStorage. Is is not intended to be used outside of the pipeline.

        :raises NotImplementedError: [description]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def find(
        self, datastream_name: str, start_time: str, end_time: str, filetype: str = None
    ) -> List[str]:
        """Finds all files of the given type from the datastream store with the
        given datastream_name and timestamps from start_time (inclusive) up to
        end_time (exclusive).  Returns a list of paths to files that match the
        criteria.

        :param datastream_name: The datastream_name as defined by ME Data Standards.
        :type datastream_name: str
        :param start_time: The start time or date to start searching for
            data (inclusive). Should be like "20210106.000000" to
            search for data beginning on or after January 6th, 2021.
        :type start_time: str
        :param end_time: The end time or date to stop searching for data
            (exclusive). Should be like "20210108.000000" to search
            for data ending before January 8th, 2021.
        :type end_time: str
        :param filetype: A file type from the DatastreamStorage.file_filters keys
            If no type is specified, all files will be returned. Defaults to None.
        :type filetype: str, optional
        :return: A list of paths in datastream storage in ascending order
        :rtype: List[str]
        """
        return

    @abc.abstractmethod
    def fetch(
        self,
        datastream_name: str,
        start_time: str,
        end_time: str,
        local_path: str = None,
        filetype: int = None,
    ):
        """Fetches files from the datastream store using the datastream_name,
        start_time, and end_time to specify the file(s) to retrieve. If the
        local path is not specified, it is up to the subclass to determine
        where to put the retrieved file(s).

        :param datastream_name: The datastream_name as defined by ME Data Standards.
        :type datastream_name: str
        :param start_time: The start time or date to start searching for
            data (inclusive). Should be like "20210106" to
            search for data beginning on or after January 6th, 2021.
        :type start_time: str
        :param end_time: The end time or date to stop searching for data
            (exclusive). Should be like "20210108" to search
            for data ending before January 8th, 2021.
        :type end_time: str
        :param local_path: The path to the directory where the data should
            be stored. Defaults to None.
        :type local_path: str, optional
        :param filetype: A file type from the DatastreamStorage.file_filters keys
            If no type is specified, all files will be returned. Defaults to None.
        :type filetype: int, optional
        :return: A list of paths where the retrieved files were stored in local storage.
            This is a context manager class, so it this method should be called via
            the 'with' statement and all files referenced by the list will be
            cleaned up when it goes out of scope.
        :rtype: DisposableLocalTempFileList:
        """
        return

    def save(
        self, dataset_or_path: Union[str, xr.Dataset], new_filename: str = None
    ) -> List[Any]:
        """Saves a local file to the datastream store.

        :param dataset_or_path: The dataset or local path to the file
            to save.  The file should be named according
            to ME Data Standards naming conventions so that this
            method can automatically parse the datastream,
            date, and time from the file name.
        :type dataset_or_path: Union[str, xr.Dataset]
        :param new_filename: If provided, the new filename to save as.
            This parameter should ONLY be provided if using
            a local path for dataset_or_path.  Must also
            follow ME Data Standards naming conventions. Defaults to None.
        :type new_filename: str, optional
        :return: A list of paths where the saved files were stored in storage.
            Path type is dependent upon the specific storage subclass.
        :rtype: List[Any]
        """
        saved_paths = []

        if isinstance(dataset_or_path, xr.Dataset):
            dataset = dataset_or_path

            # Save file for every registered output file type
            for file_extension in DatastreamStorage.output_file_extensions.values():
                dataset_filename = DSUtil.get_dataset_filename(
                    dataset, file_extension=file_extension
                )
                with self.tmp.get_temp_filepath(dataset_filename) as tmp_path:
                    FileHandler.write(dataset, tmp_path, storage=self)
                    saved_paths.append(self.save_local_path(tmp_path, new_filename))

        else:
            local_path = dataset_or_path
            saved_paths.append(self.save_local_path(local_path, new_filename))

        return saved_paths

    @abc.abstractmethod
    def save_local_path(self, local_path: str, new_filename: str = None) -> Any:
        """Given a path to a local file, save that file to the storage.

        :param local_path:  Local path to the file
            to save.  The file should be named according
            to ME Data Standards naming conventions so that this
            method can automatically parse the datastream,
            date, and time from the file name.
        :type local_path: str
        :param new_filename: If provided, the new filename to save as.
            This parameter should ONLY be provided if using
            a local path for dataset_or_path.  Must also
            follow ME Data Standards naming conventions. Defaults to None.
        :type new_filename: str, optional
        :return: The path where this file was stored in storage.
            Path type is dependent upon the specific storage subclass.
        :rtype: Any
        """
        return

    @abc.abstractmethod
    def exists(
        self, datastream_name: str, start_time: str, end_time: str, filetype: str = None
    ) -> bool:
        """Checks if any data exists in the datastream store for the provided
        datastream and time range.

        :param datastream_name: The datastream_name as defined by ME Data Standards.
        :type datastream_name: str
        :param start_time: The start time or date to start searching for
            data (inclusive). Should be like "20210106" to
            search for data beginning on or after January 6th, 2021.
        :type start_time: str
        :param end_time: The end time or date to stop searching for data
            (exclusive). Should be like "20210108" to search
            for data ending before January 8th, 2021.
        :type end_time: str
        :param filetype: A file type from the DatastreamStorage.file_filters
            keys.  If none specified, all files will be checked. Defaults to None.
        :type filetype: str, optional
        :return: True if data exists, False otherwise.
        :rtype: bool
        """
        return

    @abc.abstractmethod
    def delete(
        self, datastream_name: str, start_time: str, end_time: str, filetype: str = None
    ) -> None:
        """Deletes datastream data in the datastream store in between the
        specified time range.

        :param datastream_name: The datastream_name as defined by ME Data Standards.
        :type datastream_name: str
        :param start_time: The start time or date to start searching for
            data (inclusive). Should be like "20210106" to
            search for data beginning on or after January 6th, 2021.
        :type start_time: str
        :param end_time: The end time or date to stop searching for data
            (exclusive). Should be like "20210108" to search
            for data ending before January 8th, 2021.
        :type end_time: str
        :param filetype: A file type from the DatastreamStorage.file_filters
            keys.  If no type is specified, all files will be deleted.
            Defaults to None.
        :type filetype: str, optional
        """
        return


class DisposableLocalTempFile:
    """DisposableLocalTempFile is a context manager wrapper class for a temp file on
    the LOCAL FILESYSTEM.  It will ensure that the file is deleted when
    it goes out of scope.

    :param filepath: Path to a local temp file that could be deleted when
        it goes out of scope.
    :type filepath: str
    :param disposable: True if this file should be automatically deleted
        when it goes out of scope.  Defaults to True.
    :type disposable: bool, optional
    """

    def __init__(self, filepath: str, disposable=True):
        self.filepath = filepath
        self.disposable = disposable

    def __enter__(self):
        return self.filepath

    def __exit__(self, type, value, traceback):

        # We only clean up the file if an exception was not thrown
        if type is None and self.filepath is not None and self.disposable:
            if os.path.isfile(self.filepath):
                os.remove(self.filepath)

            elif os.path.isdir(self.filepath):
                # remove directory and all its children
                shutil.rmtree(self.filepath)


class DisposableLocalTempFileList(list):
    """Provides a context manager wrapper class for a list of
    temp files on the LOCAL FILESYSTEM.  It ensures that if
    specified, the files will be auto-deleted when the list
    goes out of scope.

    :param filepath_list: A list of local temp files
    :type filepath_list: List[str]
    :param delete_on_exception: Should the local temp files
        be deleted if an error was thrown when processing.
        Defaults to False.
    :type delete_on_exception: bool, optional
    :param disposable: Should the local temp files be auto-deleted
        when they go out of scope. Defaults to True.
    :type disposable: bool, optional
    """

    def __init__(
        self, filepath_list: List[str], delete_on_exception=False, disposable=True
    ):
        self.filepath_list = filepath_list
        self.delete_on_exception = delete_on_exception
        self.disposable = disposable

    def __enter__(self):
        return self.filepath_list

    def __exit__(self, type, value, traceback):

        if self.disposable:
            # We only clean up the files if an exception was not thrown
            if type is None or self.delete_on_exception:
                for filepath in self.filepath_list:
                    if os.path.isfile(filepath):
                        os.remove(filepath)

                    elif os.path.isdir(filepath):
                        # remove directory and all its children
                        shutil.rmtree(filepath)


class DisposableStorageTempFileList(list):
    """Provides is a context manager wrapper class for a list of
    temp files on the STORAGE FILESYSTEM.  It will ensure that the
    specified files are deleted when the list goes out of scope.

    :param filepath_list: A list of files in temporary storage area
    :type filepath_list: List[str]
    :param storage: The temporary storage service used to clean up
        temporary files.
    :type storage: TemporaryStorage
    :param disposable_files: Which of the files from the filepath_list
        should be auto-deleted when the list goes out of scope.
        Defaults to []
    :type disposable_files: list, optional
    """

    def __init__(
        self,
        filepath_list: List[str],
        storage,
        disposable_files: Union[List, None] = None,
    ):
        disposable_files = disposable_files if disposable_files is not None else list()
        self.filepath_list = filepath_list

        # Make sure that we have passed the right class
        if isinstance(storage, DatastreamStorage):
            storage = storage.tmp
        self.tmp_storage = storage
        assert isinstance(self.tmp_storage, TemporaryStorage)
        self.disposable_files = disposable_files

    def __enter__(self):
        return self.filepath_list

    def __exit__(self, type, value, traceback):

        # We only clean up the files if an exception was not thrown
        if type is None:

            # Clean up the list of files that should be removed.
            # Note that some of these files may be in temp storage
            # if they were extracted from a zip.  Others may be
            # in the input folder, which you may or may not want to
            # clean up automatically.
            #
            # TODO: this assumes that the input storage is the same
            # as the output storage.  So if the storage is on S3,
            # then the input file is also on S3.  We will have to
            # fix this for mixed-mode storage (e.g., input is on
            # local filesystem, but output is on S3)
            for filepath in self.disposable_files:
                self.tmp_storage.delete(filepath)


class TemporaryStorage(abc.ABC):
    """Each DatastreamStorage should contain a corresponding
    TemporaryStorage class which  provides
    access to a TemporaryStorage object that is used to efficiently
    handle reading/writing temporary files used during the processing
    pipeline, or to perform fileystem actions on files other than
    processed datastream files that reside in the same filesystem as
    the DatastreamStorage.

    TemporaryStorage methods return a context manager so that the
    created temporary files can be automatically removed when they go out of scope.

    TemporaryStorage is a helper class intended to be used in the internals of
    pipeline implementations only.  It is not meant as an external API for
    interacting with files in DatastreamStorage.

    TODO: rename to a more intuitive name...

    :param storage: A reference to the corresponding DatastreamStorage
    :type storage: DatastreamStorage
    """

    def __init__(self, storage: DatastreamStorage):
        self.datastream_storage = storage
        self._local_temp_folder = tempfile.mkdtemp(prefix="tsdat-pipeline-")

    @property
    def local_temp_folder(self) -> str:
        """Default method to get a local temporary folder for use when retrieving
        files from temporary storage.  This method should work for all
        filesystems, but can be overridden if needed by subclasses.

        :return: Path to local temp folder
        :rtype: str
        """
        return self._local_temp_folder

    def clean(self):
        """Clean any extraneous files from the temp working dirs.  Temp files
        could be in two places:
            #. the local temp folder - used when fetching files from the store
            #. the storage temp folder - used when extracting zip files in
               some stores (e.g., AWS)

        This method removes the local temp folder.  Child classes can
        extend this method to clean up their respective storage temp folders.
        """

        # remove any garbage files left in the local temp folder
        shutil.rmtree(self.local_temp_folder)

    def ignore_zip_check(self, filepath: str) -> bool:
        """Return true if this file should be excluded from the zip file check.
        We need this for Office documents, since they are actually zip files
        under the hood, so we don't want to try to unzip them.

        :param filepath: the file we are potentially extracting
        :type filepath: str
        :return: whether we should check if it is a zip or not
        :rtype: bool
        """
        ext = os.path.splitext(filepath)[1]
        excluded_types = [".xlsx"]
        if ext in excluded_types:
            return True

        return False

    def get_temp_filepath(
        self, filename: str = None, disposable: bool = True
    ) -> DisposableLocalTempFile:
        """Construct a filepath for a temporary file that will be located in the
        storage-approved local temp folder and will be deleted when it goes
        out of scope.

        :param filename: The filename to use for the temp file.  If no
            filename is provided, one will be created. Defaults to None
        :type filename: str, optional
        :param disposable: If true, then wrap in DisposableLocalTempfile so
            that the file will be removed when it goes out of scope.
            Defaults to True.
        :type disposable: bool, optional
        :return: Path to the local file.  The file will be
            automatically deleted when it goes out of scope.
        :rtype: DisposableLocalTempFile
        """
        if filename is None:
            now = datetime.now()
            filename = now.strftime("%Y-%m-%d.%H%M%S.%f")

        filepath = os.path.join(self.local_temp_folder, filename)
        if disposable:
            return DisposableLocalTempFile(filepath)
        else:
            return filepath

    def create_temp_dir(self) -> str:
        """Create a new, temporary directory under the local tmp area managed by
        TemporaryStorage.

        :return: Path to the local dir.
        :rtype: str
        """
        now = datetime.now()
        filename = now.strftime("%Y-%m-%d.%H%M%S.%f")
        temp_dir = os.path.join(self.local_temp_folder, filename)

        # make sure the directory exists
        os.makedirs(temp_dir, exist_ok=False)

        return temp_dir

    @abc.abstractmethod
    def extract_files(
        self, file_path: Union[str, List[str]]
    ) -> DisposableStorageTempFileList:
        """If provided a path to an archive file, this function will extract the
        archive into a temp directory IN THE SAME FILESYSTEM AS THE STORAGE.
        This means, for example that if storage was in an s3 bucket ,then
        the files would be extracted to a temp dir in that s3 bucket.  This
        is to prevent local disk limitations when running via Lambda.

        If the file is not an archive, then the same file will be returned.

        This method supports zip, tar, and tar.g file formats.

        :param file_path: The path of a file or a list
            of files that should be processed together,
            located in the same filesystem as the storage.
        :type file_path: Union[str, List[str]]
        :return: A list of paths to the files that were extracted.
            Files will be located in the temp area of the
            storage filesystem.
        :rtype: DisposableStorageTempFileList
        """
        pass

    @abc.abstractmethod
    def fetch(
        self, file_path: str, local_dir=None, disposable=True
    ) -> Union[DisposableLocalTempFile, str]:
        """Fetch a file from temp storage to a local temp folder.  If
        disposable is True, then a DisposableLocalTempFile will be returned
        so that it can be used with a context manager.

        :param file_path: The path of a file located in the same
            filesystem as the storage.
        :type file_path: str
        :param local_dir: The destination folder for the file.  If not
            specified, it will be created int the storage-approved local
            temp folder. defaults to None.
        :type local_dir: [type], optional
        :param disposable: True if this file should be auto-deleted when it
            goes out of scope. Defaults to True.
        :type disposable: bool, optional
        :return: If disposable, return a DisposableLocalTempFile, otherwise
            return the path to the local file.
        :rtype: Union[DisposableLocalTempFile, str]
        """
        pass

    @abc.abstractmethod
    def fetch_previous_file(
        self, datastream_name: str, start_time: str
    ) -> DisposableLocalTempFile:
        """Look in DatastreamStorage for the first processed file before the given date.

        :param datastream_name: The datastream_name as defined by ME Data Standards.
        :type datastream_name: str
        :param start_time: The start time or date to start searching for
            data (inclusive). Should be like "20210106" to
            search for data beginning on or after January 6th, 2021.
        :type start_time: str
        :return: If a previous file was found, return the local path to the fetched
            file. Otherwise return None.  (Return value wrapped in DisposableLocalTempFile
            so it can be auto-deleted if needed.)
        :rtype: DisposableLocalTempFile
        """
        pass

    @abc.abstractmethod
    def delete(self, file_path: str):
        """Remove a file from storage temp area if the file exists.  If the file
        does not exist, this method will NOT raise an exception.

        :param file_path: The path of a file located in the same filesystem as the storage.
        :type file_path: str
        """
        pass
