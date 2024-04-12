from .check_max import CheckMax


class CheckWarnRangeMax(CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'warn_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_range"
