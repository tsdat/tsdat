from .check_max import CheckMax


class CheckFailMax(CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'fail_max'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_max"
