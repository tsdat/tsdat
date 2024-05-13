from abc import ABC
import xarray as xr
from typing import Union

from ..base import QualityChecker


class ThresholdChecker(QualityChecker, ABC):
    """---------------------------------------------------------------------------------
    Base class for checks that use a variable attribute to specify a threshold.

    Args:
        attribute_name (str): The name of the attribute containing the maximum
            threshold. If the attribute ends in '_range' then it is assumed to be a
            list, and the first value from the list will be used as the minimum
            threshold.
        allow_equal (bool): True if values equal to the threshold should pass the check,
            False otherwise.

    ---------------------------------------------------------------------------------"""

    allow_equal: bool = True
    """True if values equal to the threshold should pass, False otherwise."""

    attribute_name: str
    """The attribute on the data variable that should be used to get the threshold."""

    def _get_threshold(
        self,
        dataset: xr.Dataset,
        variable_name: str,
        min_: bool,
    ) -> Union[float, None]:
        threshold = dataset[variable_name].attrs.get(self.attribute_name, None)
        if threshold is not None:
            if isinstance(threshold, list):
                index = 0 if min_ else -1
                threshold = threshold[index]
        return threshold
