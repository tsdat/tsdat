from .check_max import CheckMax


class CheckValidRangeMax(CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'valid_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_range"
