import os
import abc
from tsdat.config import Config
from tsdat.io.storage import DatastreamStorage
from tsdat.io.file_handlers import FILEHANDLERS, FileHandler


class Pipeline(abc.ABC):

    def __init__(self, config: Config, storage: DatastreamStorage) -> None:
        self.storage = storage
        self.config = config
        pass

    @abc.abstractmethod
    def run(self, filepath: str):
        return
    
    def get_filehandler(self, file_path: str) -> FileHandler:
        """-------------------------------------------------------------------
        Retrieves the appropriate FileHandler for a given file. 

        Args:
            file_path (str):    The complete path to the file requiring a 
                                FileHandler.

        Raises:
            KeyError:   Raises KeyError if no FileHandler has been defined for
                        the the file provided.

        Returns:
            FileHandler: The FileHandler class to use for the provided file.
        -------------------------------------------------------------------"""
        _, ext = os.path.splitext(file_path)
        if ext not in FILEHANDLERS:
            raise KeyError(f"no FileHandler for extension: {ext}")
        return FILEHANDLERS[ext]
 