import act
import xarray as xr


@xr.register_dataset_accessor('TEST')
class TEST(object):
    CONST1 = "1"
    CONST2 = "2"


class ATTS(object):
    """-------------------------------------------------------------------
    Class that adds methods for interacting with tsdat data model
    specific attributes.
    -------------------------------------------------------------------"""
    # Symbolic constants for global attributes
    TITLE = 'title'
    DESCRIPTION = 'description'
    CONVENTIONS = 'conventions'
    HISTORY = 'history'
    DOI = 'doi'
    INSTITUTION = 'institution'
    CODE_URL = 'code_url'
    REFERENCES = 'references'
    INPUT_FILES = 'input_files'
    LOCATION_ID = 'location_id'
    DATASTREAM = 'datastream'
    DATA_LEVEL = 'data_level'
    LOCATION_DESCRPTION = 'location_description'
    INSTRUMENT_NAME = 'instrument_name'
    SERIAL_NUMBER = 'serial_number'
    INSTRUMENT_DESCRPTION = 'instrument_description'
    INSTRUMENT_MANUFACTURER = 'instrument_manufacturer'
    AVERAGING_INTERVAL = 'averaging_interval'
    SAMPLING_INTERVAL = 'sampling_interval'

    # Symbolic constants for variable attributes
    UNITS = 'units'
    VALID_DELTA = 'valid_delta'
    FAIL_RANGE = 'fail_range'
    WARN_RANGE = 'warn_range'
    FILL_VALUE = '_FillValue'

    @staticmethod
    def get_missing_value(ds: xr.Dataset, variable_name):
        return act.utils.get_missing_value(ds, variable_name, use_FillValue=True)

    @staticmethod
    def get_fail_min(ds: xr.Dataset, variable_name):
        pass

    @staticmethod
    def get_fail_max(ds: xr.Dataset, variable_name):
        pass

    @staticmethod
    def get_warn_min(ds: xr.Dataset, variable_name):
        pass

    @staticmethod
    def get_warn_max(ds: xr.Dataset, variable_name):
        pass