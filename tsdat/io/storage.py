import abc
import os
import re
import shutil
import tempfile
import xarray as xr
import yaml
from datetime import datetime
from typing import List, Union, Any

from tsdat.config.utils import instantiate_handler, configure_yaml
from tsdat.io import FileHandler
from tsdat.utils import DSUtil


def _is_image(x):
    return True if DSUtil.is_image(x.__str__()) else False


def _is_raw(x):
    return True if '.raw.' in x.__str__() else False


class DatastreamStorage(abc.ABC):

    default_file_type = None

    file_filters = {
        'plots': _is_image,
        'raw': _is_raw
    }

    output_file_extensions = {
    }

    @staticmethod
    def from_config(storage_config_file: str):
        """-------------------------------------------------------------------
        Load a yaml config file which provides the storage constructor
        parameters.

        Args:
            storage_config_file (str): The path to the config file to load

        Returns:
            DatastreamStorage: An subclass instance created from the config file.
        -------------------------------------------------------------------"""
        # Add the config folder as a special environment parameter before we
        # load the yaml file
        config_folder = os.path.dirname(storage_config_file)
        os.environ['CONFIG_DIR'] = config_folder

        # Configure yaml to substitute environment variables
        configure_yaml()

        # Load the config file
        with open(storage_config_file, 'r') as file:
            storage_dict = yaml.load(file, Loader=yaml.SafeLoader).get('storage', {})

        # Now instantiate the storage
        storage = instantiate_handler(handler_desc=storage_dict)

        # Now we need to register all the file handlers
        # First do the inputs
        input_handlers = storage_dict.get('file_handlers',{}).get('input', {})
        for handler_dict in input_handlers.values():
            handler = instantiate_handler(handler_desc=handler_dict)
            FileHandler.register_file_handler(handler_dict['file_pattern'], handler)

        # Now the outputs
        output_handlers = storage_dict.get('file_handlers', {}).get('output', {})
        for key, handler_dict in output_handlers.items():
            file_extension = handler_dict['file_extension']
            file_pattern = f".*\\{file_extension}"

            # First register the writers
            handler = instantiate_handler(handler_desc=handler_dict)
            FileHandler.register_file_handler(file_pattern, handler)

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

    def __init__(self, parameters={}):
        self.parameters = parameters
        retain_input_files = parameters.get('retain_input_files', 'False')
        if type(retain_input_files) == str:
            retain_input_files = False if retain_input_files.lower() == 'false' else True

        self.remove_input_files = not retain_input_files

    @property
    def tmp(self):
        """-------------------------------------------------------------------
        Each subclass should define the tmp property, which provides
        access to a TemporaryStorage object that is used to efficiently
        handle reading writing temporary files used during the processing
        pipeline.  Is is not intended to be used outside of the pipeline.
        -------------------------------------------------------------------"""
        raise NotImplementedError

    @abc.abstractmethod
    def find(self, datastream_name: str, start_time: str, end_time: str,
             filetype: str = None) -> List[str]:
        """-------------------------------------------------------------------
        Finds all files of the given type from the datastream store with the
        given datastream_name and timestamps from start_time (inclusive) up to
        end_time (exclusive).  Returns a list of paths to files that match the
        criteria.

        Args:
            datastream_name (str):  The datastream_name as defined by
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106.000000" to
                                search for data beginning on or after
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108.000000" to search
                            for data ending before January 8th, 2021.

            filetype (str): A file type from the DatastreamStorage.file_filters keys
                            If no type is specified, all files will be returned.

        Returns:
            List[str]:  A list of paths in datastream storage in ascending order
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def fetch(self, datastream_name: str, start_time: str, end_time: str,
              local_path: str = None, filetype: int = None) -> List[str]:
        """-------------------------------------------------------------------
        Fetches files from the datastream store using the datastream_name,
        start_time, and end_time to specify the file(s) to retrieve. If the 
        local path is not specified, it is up to the subclass to determine
        where to put the retrieved file(s).

        Args:
            datastream_name (str):  The datastream_name as defined by 
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106" to
                                search for data beginning on or after 
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108" to search
                            for data ending before January 8th, 2021.
            local_path (str):   The path to the directory where the data
                                should be stored.
            filetype (int):   A file type from the DatastreamStorage.FILE_TYPE
                              list.  If no type is specified, all files will
                              be returned.

        Returns:
            DisposableStorageTempFileList:  A list of paths where
                                the retrieved files were stored in local storage.
        -------------------------------------------------------------------"""
        return

    def save(self, dataset_or_path: Union[str, xr.Dataset], new_filename: str = None) -> List[Any]:

        """-------------------------------------------------------------------
        Saves a local file to the datastream store.

        Args:
            dataset_or_path (str):   The dataset or local path to the file
                                to save.  The file should be named according
                                to MHKiT-Cloud naming conventions so that this
                                method canautomatically parse the datastream,
                                date, and time from the file name.
            new_filename (str): If provided, the new filename to save as.
                                This parameter should ONLY be provided if using
                                a local path for dataset_or_path.  Must also
                                follow MHKIT-Cloud naming conventions.

        Returns:
            List[Any]:          A list of paths where the saved files were stored
                                in storage.
        -------------------------------------------------------------------"""
        saved_paths = []

        if isinstance(dataset_or_path, xr.Dataset):
            dataset = dataset_or_path

            # Save file for every registered output file type
            for file_extension in DatastreamStorage.output_file_extensions.values():
                dataset_filename = DSUtil.get_dataset_filename(dataset, file_extension=file_extension)
                with self.tmp.get_temp_filepath(dataset_filename) as tmp_path:
                    FileHandler.write(dataset, tmp_path)
                    saved_paths.append(self.save_local_path(tmp_path, new_filename))

        else:
            local_path = dataset_or_path
            saved_paths.append(self.save_local_path(local_path, new_filename))

        return saved_paths

    @abc.abstractmethod
    def save_local_path(self, local_path: str, new_filename: str = None) -> Any:
        return

    @abc.abstractmethod
    def exists(self, datastream_name: str, start_time: str, end_time: str,
               filetype: str = None) -> bool:
        """-------------------------------------------------------------------
        Checks if any data exists in the datastream store for the provided
        datastream and time range.

        Args:
            datastream_name (str):  The datastream_name as defined by 
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106" to
                                search for data beginning on or after 
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108" to search
                            for data ending before January 8th, 2021.
            filetype (str):  A file type from the DatastreamStorage.file_filters
                             keys.  If none specified, all files will be checked.

        Returns:
            bool: True if data exists, False otherwise.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def delete(self, datastream_name: str, start_time: str, end_time: str,
               filetype: str = None) -> None:
        """-------------------------------------------------------------------
        Deletes datastream data in the datastream store in between the 
        specified time range. 

        Args:
            datastream_name (str):  The datastream_name as defined by 
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106" to
                                search for data beginning on or after 
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108" to search
                            for data ending before January 8th, 2021.
            filetype (str):  A file type from the DatastreamStorage.file_filters
                             keys.  If no type is specified, all files will
                             be deleted.
        -------------------------------------------------------------------"""
        return


class DisposableLocalTempFile:
    """-------------------------------------------------------------------
    DisposableLocalTempFile is a context manager wrapper class for a temp file on
    the LOCAL FILESYSTEM.  It will ensure that the file is deleted when
    it goes out of scope.
    -------------------------------------------------------------------"""
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


class DisposableLocalTempFileList (list):
    """-------------------------------------------------------------------
    Provides a context manager wrapper class for a list of
    temp files on the LOCAL FILESYSTEM.  It will ensure that the files
    are deleted when the list goes out of scope.
    -------------------------------------------------------------------"""

    def __init__(self, filepath_list: List[str], delete_on_exception=False, disposable=True):
        """-------------------------------------------------------------------
        Args:
            filepath_list (List[str]):   A list of paths to files in temporary
                                         storage.

            delete_on_exception:        The default behavior is to not remove
                                        the files on exit if an exception
                                        occurs.  However, users can override this
                                        setting to force files to be cleaned up
                                        no matter if an exception is thrown or
                                        not.
            disposable:                 True if this file should be auto-deleted
                                        when out of scope.
        -------------------------------------------------------------------"""
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


class DisposableStorageTempFileList (list):
    """-------------------------------------------------------------------
    Provides is a context manager wrapper class for a list of
    temp files on the STORAGE FILESYSTEM.  It will ensure that the files
    are deleted when the list goes out of scope.
    -------------------------------------------------------------------"""

    def __init__(self, filepath_list: List[str], storage, disposable_files=[]):
        """-------------------------------------------------------------------
        Args:
            filepath_list (List[str]):   A list of paths to files in temporary
                                         storage.

            storage (TemporaryStorage): The temporary storage service used
                                        to clean up temporary files.

            disposable_files:           List of files that should be removed
                                        when this list goes out of scope.
        -------------------------------------------------------------------"""
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
    """-------------------------------------------------------------------
    TemporaryStorage is used to efficiently handle reading writing temporary
    files used during the processing pipeline.  TemporaryStorage methods
    return a context manager so that the created temporary files can be
    automatically removed when they go out of scope.
    -------------------------------------------------------------------"""

    def __init__(self, storage: DatastreamStorage):
        """-------------------------------------------------------------------
        Args:
            storage (DatastreamStorage): A reference to the corresponding
                                         DatastreamStorage
        -------------------------------------------------------------------"""
        self.datastream_storage = storage
        self._local_temp_folder = tempfile.mkdtemp(prefix='tsdat-pipeline-')

    @property
    def local_temp_folder(self) -> str:
        """-------------------------------------------------------------------
        Default method to get a local temporary folder for use when retrieving
        files from temporary storage.  This method should work for all
        filesystems, but can be overridden if needed by subclasses.

        Returns:
            str:   Path to local temp folder
        -------------------------------------------------------------------"""
        return self._local_temp_folder

    def clean(self):
        """-------------------------------------------------------------------
        Clean any extraneous files from the temp working dirs.  Temp files
        could be in two places:
           1) the local temp folder - used when fetching files from the store
           2) the storage temp folder - used when extracting zip files in
              some stores (e.g., AWS)

        This method removes the local temp folder.  Child classes can
        extend this method to clean up their respective storage temp folders.
        -------------------------------------------------------------------"""
        # remove any garbage files left in the local temp folder
        shutil.rmtree(self.local_temp_folder)


    def get_temp_filepath(self, filename: str = None, disposable: bool = True) -> DisposableLocalTempFile:
        """-------------------------------------------------------------------
        Construct a filepath for a temporary file that will be located in the
        storage-approved local temp folder and will be deleted when it goes
        out of scope.

        Args:
            filename (str):   The filename to use for the temp file.  If no
                              filename is provided, one will be created.

            disposable (bool): If true, then wrap in DisposableLocalTempfile so
                               that the file will be removed when it goes out of
                               scope

        Returns:
            DisposableLocalTempFile:   Path to the local file.  The file will be
                                       automatically deleted when it goes out
                                       of scope.
        -------------------------------------------------------------------"""
        if filename is None:
            now = datetime.now()
            filename = now.strftime("%Y-%m-%d.%H%M%S.%f")

        filepath = os.path.join(self.local_temp_folder, filename)
        if disposable:
            return DisposableLocalTempFile(filepath)
        else:
            return filepath

    def create_temp_dir(self) -> str:
        """-------------------------------------------------------------------
        Create a new, temporary directory under the local tmp area managed by
        TemporaryStorage.

        Returns:
            str:   Path to the local dir.
        -------------------------------------------------------------------"""
        now = datetime.now()
        filename = now.strftime("%Y-%m-%d.%H%M%S.%f")
        temp_dir = os.path.join(self.local_temp_folder, filename)

        # make sure the directory exists
        os.makedirs(temp_dir, exist_ok=False)

        return temp_dir

    @abc.abstractmethod
    def extract_files(self, file_path: Union[str, List[str]]) -> DisposableStorageTempFileList:
        """-------------------------------------------------------------------
        If provided a path to an archive file, this function will extract the
        archive into a temp directory IN THE SAME FILESYSTEM AS THE STORAGE.
        This means, for example that if storage was in an s3 bucket ,then
        the files would be extracted to a temp dir in that s3 bucket.  This
        is to prevent local disk limitations when running via Lambda.

        If the file is not an archive, then the same file will be returned.

        This method supports zip, tar, and tar.g file formats.

        Args:
            file_path (Union[str, List[str]]):   The path of a file or a list
                                  of files that should be processed together,
                                  located in the same filesystem as the storage.

        Returns:
            DisposableStorageTempFileList:  A list of paths to the files that were extracted.
                                  Files will be located in the temp area of the
                                  storage filesystem.
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def fetch(self, file_path: str, local_dir=None, disposable=True) -> Union[DisposableLocalTempFile, str]:
        """-------------------------------------------------------------------
        Fetch a file from temp storage to a local temp folder.  If
        disposable is True, then a DisposableLocalTempFile will be returned
        so that it can be used with a context manager.

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.
            local_dir(str):    The destination folder for the file.  If not
                               specified, it will be created.
            disposable (bool):

        Returns:
            DisposableLocalTempFile | str:   The local path to the file
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def fetch_previous_file(self, datastream_name: str, start_time) -> DisposableLocalTempFile:
        """-------------------------------------------------------------------
        Look in DatastreamStorage for the first file before the given date.

        Args:
            datastream_name (str):
            start_time (str):

        Returns:
            DisposableLocalTempFile:          The local path to the file
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def delete(self, file_path: str) -> None:
        """-------------------------------------------------------------------
        Remove a file from storage temp area if the file exists.  If the file
        does not exists, this method will NOT raise an exception.

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.
        -------------------------------------------------------------------"""
        pass

