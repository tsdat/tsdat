from .check_max import CheckMax


class CheckWarnMax(CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'warn_max'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_max"
