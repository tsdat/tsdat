
class DatastreamStorage:

    # Fetch a file from the datastream store
    # If the local path isn't specified, it is up to the
    # subclass to determine where to put the file
    def fetch(self, datastream_store_path:str, local_path:str):
        # Return the path to the local file
        raise NotImplementedError
    
    # Save a local file to the datastream store
    def save(self, local_path:str, datastream_store_path:str):
        raise NotImplementedError

    # Check if the file exists in the datastream store
    def exists(self, datastream_store_path:str):
        # Return boolean
        raise NotImplementedError

    # Delete a file in the datastream store
    def delete(self, datastream_store_path:str):
        raise NotImplementedError

    # List the contents of the specified path. Return a 2-tuple of lists:
    # the first item being directories, the second item being files.
    # (this method may not be needed)
    def listdir(self, datastream_store_path:str):
        raise NotImplementedError


class FilesystemStorage(DatastreamStorage):
    """[summary]

    Args:
        DatastreamStorage ([type]): [description]
    """
    def __init__(self, root):
      self.__root = root

