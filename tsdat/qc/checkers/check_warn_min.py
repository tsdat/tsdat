from .check_min import CheckMin


class CheckWarnMin(CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'warn_min'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_min"
