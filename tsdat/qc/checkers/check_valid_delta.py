from .check_delta import CheckDelta


class CheckValidDelta(CheckDelta):
    """------------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than 'valid_delta'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_delta"
