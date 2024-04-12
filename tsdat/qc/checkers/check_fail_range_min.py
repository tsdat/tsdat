from .check_min import CheckMin


class CheckFailRangeMin(CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'fail_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_range"
