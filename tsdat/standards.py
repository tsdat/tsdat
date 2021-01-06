from typing import List


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
            if not (char.isalpha() or char in [".", "-", "_"]):
                raise ValueError(f"'{char}' is not a permitted in datastream_name.")
        return
        
    @staticmethod
    def get_datastream_path(datastream_name: str, root: str = None) -> str:
        """-------------------------------------------------------------------
        Returns the path to the parent directory relative to the root 
        (optional) of where the datastream should be stored according to 
        MHKiT-Cloud Data Standards. 

        Args:
            datastream_name (str):  The name of the datastream.
            root (str, optional):   The root of the path to return. Defaults 
                                    to None.

        Returns:
            str: The path to the directory where the data should be saved.
        -------------------------------------------------------------------"""
        Standards.validate_datastream_name(datastream_name)
        location_id = datastream_name.split(".")[0]
        return f"{root}/{location_id}/{datastream_name}"

