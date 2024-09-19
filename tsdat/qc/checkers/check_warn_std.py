from .check_std import CheckStd


class CheckWarnStd(CheckStd):
    """---------------------------------------------------------------------------------
    Checks for data points lying beyond more standard deviations from the mean than
    'warn_n_std'.

    ---------------------------------------------------------------------------------"""

    attribute_name: str = "warn_std"
