from .check_max import CheckMax


class CheckFailRangeMax(CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'fail_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_range"
