from .check_min import CheckMin


class CheckValidMin(CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'valid_min'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_min"
