from .check_std import CheckStd


class CheckFailStd(CheckStd):
    """---------------------------------------------------------------------------------
    Checks for data points lying beyond more standard deviations from the mean than
    'fail_n_std'.

    ---------------------------------------------------------------------------------"""

    attribute_name: str = "fail_std"
