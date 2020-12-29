import functools
import xarray as xr
from typing import List

READERS = dict()

def register_filereader(file_extension: str):
    """-----------------------------------------------------------------------
    Python decorator to register a function in the READERS dictionary. 

    Args:
        file_extension (str): [description]
    -----------------------------------------------------------------------"""
    def decorator_register(func):
        if isinstance(file_extension, List):
            for ext in file_extension:
                READERS[ext] = func
        else:
            READERS[file_extension] = func
        @functools.wraps(func)
        def wrapper_register(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper_register
    return decorator_register

def netcdf_reader(filepath: str) -> xr.Dataset:
    return xr.open_dataset(filepath)

@register_filereader([".txt", ".md"])
def txt_reader(filepath: str) -> List[str]:
    """-----------------------------------------------------------------------
    FileReader for a text (.txt) file. Opens the provided file and returns a 
    list of lines in the file.

    Args:
        filepath (str): The path to the file to read.

    Returns:
        List[str]: A list of the lines in the file.
    -----------------------------------------------------------------------"""
    with open(filepath) as file:
        lines = file.readlines()
    return lines


@register_filereader(".nc")
class NetcdfReader:
    def read():
        print("read")
    def write():
        print("write")