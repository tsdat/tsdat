from .check_delta import CheckDelta


class CheckFailDelta(CheckDelta):
    """------------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than 'fail_delta'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_delta"
