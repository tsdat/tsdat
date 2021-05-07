import seaborn
from typing import List

import abc

import numpy as np
import xarray as xr


class QualityHandler(abc.ABC):
    """
    Class containing code to be executed if a particular quality check fails.   
    """

    def __init__(self, ds: xr.Dataset, previous_data: xr.Dataset, quality_manager, parameters={}):
        """Summary line.

        Extended description of function.

        :param int arg1: Description of arg1.
        :param str arg2: Description of arg2.
        :raise: ValueError if arg1 is equal to arg2
        :return: Description of return value
        :rtype: bool

        :example:

        >>> a=1
        >>> b=2
        >>> func(a,b)
        True

        .. code-block:: python
           :linenos:

           a=1
           b=2
           func(a,b)
        """
        self.ds = ds
        self.previous_data = previous_data
        self.quality_manager = quality_manager
        self.params = parameters


    @abc.abstractmethod
    def run(self, variable_name: str, results_array: np.ndarray):
        """Summary line.

        Extended description of function.

        :param int arg1: Description of arg1.
        :param str arg2: Description of arg2.
        :raise: ValueError if arg1 is equal to arg2
        :return: Description of return value
        :rtype: bool

        :example:

        >>> a=1
        >>> b=2
        >>> func(a,b)
        True

        .. code-block:: python
           :linenos:

           a=1
           b=2
           func(a,b)
        """
        pass

    def record_correction(self, variable_name: str):
        """Summary line.

        Extended description of function.

        :param int arg1: Description of arg1.
        :param str arg2: Description of arg2.
        :raise: ValueError if arg1 is equal to arg2
        :return: Description of return value
        :rtype: bool

        :example:

        >>> a=1
        >>> b=2
        >>> func(a,b)
        True

        .. code-block:: python
           :linenos:

           a=1
           b=2
           func(a,b)
        """
        correction = self.params.get("correction", None)
        if correction is not None:
            pass


def hello_world(name: str) -> List[str]:
    """

    :param name:
    :type name:
    :return:
    :rtype:
    """

    return [f"hello world {name}"]

