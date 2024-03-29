from .check_max import CheckMax


class CheckValidMax(CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'valid_max'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_max"
