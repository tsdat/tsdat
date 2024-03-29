from .check_delta import CheckDelta


class CheckWarnDelta(CheckDelta):
    """------------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than 'warn_delta'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_delta"
