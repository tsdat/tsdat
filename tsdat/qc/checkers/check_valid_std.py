from .check_std import CheckStd


class CheckValidStd(CheckStd):
    """---------------------------------------------------------------------------------
    Checks for data points lying beyond more standard deviations from the mean than
    'valid_n_std'.

    ---------------------------------------------------------------------------------"""

    attribute_name: str = "valid_std"
