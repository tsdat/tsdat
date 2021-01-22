import os
import xarray as xr
from typing import List
from tsdat import Config

class Standards:
    """-----------------------------------------------------------------------
    Class to encode MHKIT-CLOUD Data Standards. Provides a number of utility
    methods for validating data, filenames, and more conform with the defined
    standards.
    -----------------------------------------------------------------------"""
    
    @staticmethod
    def validate_datastream_name(datastream_name: str) -> None:
        """-------------------------------------------------------------------
        Validates the provided datastream_name. Raises a ValueError if the 
        datastream_name does not conform with standards, None otherwise.

        Args:
            datastream_name (str): The datastream_name to check.

        Raises:
            ValueError: Raises ValueError if the datastream_name is not legal
        -------------------------------------------------------------------"""
        components = datastream_name.split(".")
        if len(components) != 3:
            raise ValueError("datastream_name must be like: (location_id).(instrument_id)(qualifier)(temporal).(data_level)")
        for char in datastream_name:
            if not (char.isalnum() or char in [".", "-", "_"]):
                raise ValueError(f"'{char}' is not a permitted character in datastream_name.")
        return
    
    @staticmethod
    def validate_filename(filename: str) -> None:
        """-------------------------------------------------------------------
        Validates the provided filename. Raises a ValueError if the filename
        does not conform with standards, None otherwise.

        Args:
            filename (str): The filename to check

        Raises:
            ValueError: Raises ValueError if the filename is not legal
        -------------------------------------------------------------------"""
        components = filename.split(".")
        datastream_name = ".".join(components[:3])
        date = components[3]
        time = components[4]
        ext = components[5]
        Standards.validate_datastream_name(datastream_name)
        # TODO: break these checks apart into their own methods with more complete handling.
        if not (date.isnumeric() and len(date) == 8):
            raise ValueError(f"'{date}' is not a valid date")
        if not (time.isnumeric() and len(time) == 6):
            raise ValueError(f"'{time}' is not a valid time")
        if ext not in ["nc", "csv", "yaml", "metadata", "parquet", "raw"]:
            raise ValueError(f"'{ext}' is not a valid file extension.")
        return

    @staticmethod
    def get_datastream_path(datastream_name: str, root: str = "") -> str:
        """-------------------------------------------------------------------
        Returns the path to the parent directory relative to the root 
        (optional) of where the datastream should be stored according to 
        MHKiT-Cloud Data Standards. 

        Args:
            datastream_name (str, optional):    The datastream_name.
            root (str, optional):   The root of the path to return. Defaults 
                                    to an empty string.

        Returns:
            str: The path to the directory where the data should be saved.
        -------------------------------------------------------------------"""
        Standards.validate_datastream_name(datastream_name)
        location_id = datastream_name.split(".")[0]
        return os.path.join(root, location_id, datastream_name)

    @staticmethod
    def validate(dataset: xr.Dataset):
        """-------------------------------------------------------------------
        Confirms that the dataset conforms with MHKiT-Cloud data standards. 
        Raises an error if the dataset is improperly formatted. This method 
        should be overridden if different standards or validation checks 
        should be applied.

        Args:
            dataset (xr.Dataset): The dataset to validate.
        -------------------------------------------------------------------"""
        # We need to check that the coordinate dimensions are valid
        # We need to check that any unlimited dimension has a coordinate variable
        # We need to check that the type of variable attributes:
        # _FillValue, valid_range, fail_range,
        # fail_max, warn_range, warn_max, and valid_delta are all the same
        # data type as the corresponding variable

        # TODO: should the standards automatically assign a default _FillValue for
        # each variable, even if none is defined in the config?
        pass
