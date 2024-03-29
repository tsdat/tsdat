from .check_min import CheckMin


class CheckFailMin(CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'fail_min'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_min"
