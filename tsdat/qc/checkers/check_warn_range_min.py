from .check_min import CheckMin


class CheckWarnRangeMin(CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'warn_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_range"
