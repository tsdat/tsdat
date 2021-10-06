class VARS:
    """Class that adds keywords for referring to variables."""

    ALL = "ALL"
    COORDS = "COORDS"
    DATA_VARS = "DATA_VARS"


class ATTS:
    """Class that adds constants for interacting with tsdat data-model
    specific attributes."""

    # Symbolic constants for global attributes
    TITLE = "title"
    DESCRIPTION = "description"
    CONVENTIONS = "conventions"
    HISTORY = "history"
    DOI = "doi"
    INSTITUTION = "institution"
    CODE_URL = "code_url"
    REFERENCES = "references"
    INPUT_FILES = "input_files"
    LOCATION_ID = "location_id"
    DATASTREAM = "datastream_name"
    DATA_LEVEL = "data_level"
    LOCATION_DESCRPTION = "location_description"
    INSTRUMENT_NAME = "instrument_name"
    SERIAL_NUMBER = "serial_number"
    INSTRUMENT_DESCRPTION = "instrument_description"
    INSTRUMENT_MANUFACTURER = "instrument_manufacturer"
    AVERAGING_INTERVAL = "averaging_interval"
    SAMPLING_INTERVAL = "sampling_interval"

    # Symbolic constants for variable attributes
    UNITS = "units"
    VALID_DELTA = "valid_delta"
    VALID_RANGE = "valid_range"
    FAIL_RANGE = "fail_range"
    WARN_RANGE = "warn_range"
    FILL_VALUE = "_FillValue"
    CORRECTIONS_APPLIED = "corrections_applied"
