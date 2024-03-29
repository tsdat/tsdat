from .check_min import CheckMin


class CheckValidRangeMin(CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'valid_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_range"
