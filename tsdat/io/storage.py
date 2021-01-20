import os
import abc
import shutil
import zipfile
import tarfile
import datetime
import boto3
from typing import List, Dict
from tsdat.standards import Standards
from tsdat.utils import DSUtil

class DatastreamStorage(abc.ABC):

    @abc.abstractmethod
    def fetch(self, datastream_name: str, start_time: str, end_time: str, local_path: str = None) -> List[str]:
        """-------------------------------------------------------------------
        Fetches a file from the datastream store using the datastream_name, 
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

        Returns:
            List[str]:  A list of paths where the retrieved files were stored
                        in local storage.  
        -------------------------------------------------------------------"""
        return
    
    @abc.abstractmethod
    def save(self, local_paths: List[str]) -> None:
        """-------------------------------------------------------------------
        Saves a local file to the datastream store.

        Args:
            local_path (str):   The path to the local file to save. The file 
                                should be named according to MHKiT-Cloud 
                                naming conventions so that this method can
                                automatically parse the datastream, start,
                                and end times from the file name.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def exists(self, datastream_name: str, start_time: str, end_time: str) -> bool:
        """-------------------------------------------------------------------
        Checks if data exists in the datastream store for the provided 
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

        Returns:
            bool: True if data exists, False otherwise.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def delete(self, datastream_name: str, start_time: str, end_time: str) -> None:
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
        -------------------------------------------------------------------"""
        return

    # need a temp storage area and some storage utils that allow us
    # to unzip files from one bucket to another or to move files from
    # temp area to persistent area
    @abc.abstractmethod
    def unzip(self, file_path: str) -> List:
        # return list of file paths that were extracted
        # can handle zip or tar file formats
        pass

    @abc.abstractmethod
    def save(self, file_path: str) -> None:
        pass

    @abc.abstractmethod
    def delete(self, file_path: str) -> None:
        pass


class AwsStorage(DatastreamStorage):

    def __init__(self, region: str = None, bucket: str = None):
        assert region
        assert bucket
        self.bucket = bucket
        self.region = region

    @staticmethod
    def get_s3_path(region: str, bucket_name: str = None, key: str = None):
        assert bucket_name
        assert key

        # We are creating our own string to hold the region, bucket & key, since
        # boto3 needs all three in order to access a file
        # s3_client = boto3.client('s3', region_name='eu-central-1')
        # s3_client.download_file(bucket, key, download_path)
        return f"Region:{region} Bucket:{bucket_name} Key:{key}"

    def fetch(self, datastream_name: str, start_time: str, end_time: str, local_path: str = None) -> List[str]:
        pass

    def save(self, local_paths: List[str]) -> None:
        pass

    def exists(self, datastream_name: str, start_time: str, end_time: str) -> bool:
        pass

    def delete(self, datastream_name: str, start_time: str, end_time: str) -> None:
        pass



class FilesystemStorage(DatastreamStorage):
    """-----------------------------------------------------------------------
    DatastreamStorage subclass for a typical Linux-based filesystem.
    -----------------------------------------------------------------------"""

    def __init__(self, root: str = ""):
      self.__root = root

    @staticmethod
    def get_date_from_filename(filename: str) -> str:
        """-------------------------------------------------------------------
        Given a filename that conforms to MHKiT-Cloud Data Standards, return 
        the date of the first point of data in the file. 

        Args:
            filename (str): The filename or path to the file.

        Returns:
            str: The date, in "yyyymmdd.hhmmss" format.
        -------------------------------------------------------------------"""
        filename = os.path.basename(filename)
        date = filename.split(".")[1]
        time = filename.split(".")[2]
        return f"{date}.{time}"

    def fetch(self, datastream_name: str, start_time: str, end_time: str, local_path: str = None) -> List[str]:
        """-------------------------------------------------------------------
        Fetches a file from the filesystem store using the datastream_name, 
        start_time, and end_time to specify the file(s) to retrieve. The 
        retrieved files will be saved in the directory `local_path`.

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

        Returns:
            List[str]:  A list of paths where the retrieved files were stored
                        in local storage.  
        -------------------------------------------------------------------"""
        store_dir = Standards.get_datastream_path(datastream_name, root=self.__root)
        if not os.path.isdir(store_dir):
            return []
        files = [f for f in os.listdir(store_dir) if start_time <= self.get_date_from_filename(f) < end_time]
        sources = [os.path.join(store_dir, file) for file in files]
        targets = [os.path.join(local_path, file) for file in files]
        for source, target in zip(sources, targets):
            shutil.copy(source, target)
        return targets
    
    def save(self, local_paths: List[str]) -> None:
        """-------------------------------------------------------------------
        Saves a local file to the appropriate location in the filesystem.

        Args:
            local_path (str):   The path to the local file to save. The file 
                                should be named according to MHKiT-Cloud 
                                naming conventions.
        -------------------------------------------------------------------"""
        if isinstance(local_paths, str):
            local_paths = [local_paths]
        for local_path in local_paths:
            filename = os.path.basename(local_path)
            datastream_name = ".".join(filename.split(".")[:3])
            data_dir = Standards.get_datastream_path(datastream_name) # relative to __root
            target_dir = os.path.join(self.__root, data_dir) # includes __root
            target_path = os.path.join(target_dir, filename)
            os.makedirs(target_dir, exist_ok=True)
            shutil.copy(local_path, target_path)
    
    def exists(self, datastream_name: str, start_time: str, end_time: str) -> bool:
        """-------------------------------------------------------------------
        Checks if data exists in the filesystem for the provided datastream 
        and time range.

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

        Returns:
            bool: True if data exists, False otherwise.
        -------------------------------------------------------------------"""
        dir_to_check = Standards.get_datastream_path(datastream_name=datastream_name, root=self.__root)
        for file in os.listdir(dir_to_check):
            if start_time <= self.get_date_from_filename(file) < end_time:
                return True
        return False
    
    def _find(self, datastream_name: str, start_time: str, end_time: str) -> List[str]:
        """-------------------------------------------------------------------
        Returns paths to the datastream's files in the filesystem where the 
        start and end times of the data files fall within the provided 
        start_time and end_time range.

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

        Returns:
            bool: True if data exists, False otherwise.
        -------------------------------------------------------------------"""
        dir_to_check = Standards.get_datastream_path(datastream_name=datastream_name, root=self.__root)
        storage_paths = []
        for file in os.listdir(dir_to_check):
            if start_time <= self.get_date_from_filename(file) < end_time:
                storage_paths.append(os.path.join(dir_to_check, file))
        return storage_paths

    def delete(self, datastream_name: str, start_time: str, end_time: str) -> None:
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
        -------------------------------------------------------------------"""
        files_to_delete = self._find(datastream_name, start_time, end_time)
        for file in files_to_delete:
            os.remove(file)
        return

