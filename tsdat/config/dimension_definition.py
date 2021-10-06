from typing import Union


class DimKeys:
    """Class that provides a handle for keys in the Dimensions section fo the
    dataset_definition"""

    LENGTH = "length"


class DimensionDefinition:
    """Class to represent dimensions defined in the pipeline config file.

    :param name: The name of the dimension
    :type name: str
    :param length:
        The length of the dimension. This should be one of:
        ``"unlimited"``, ``"variable"``, or a positive `int`. The 'time'
        dimension should always have length of ``"unlimited"``.
    :type length: Union[str, int]
    """

    def __init__(self, name: str, length: Union[str, int]):
        self.name: str = name
        self.length: str = length.get(DimKeys.LENGTH, None)

    def is_unlimited(self) -> bool:
        """Returns ``True`` is the dimension has unlimited length. Represented by
        setting the length attribute to ``"unlimited"``.

        :return: ``True`` if the dimension has unlimited length.
        :rtype: bool
        """
        return self.length == "unlimited"

    def is_variable_length(self) -> bool:
        """Returns ``True`` if the dimension has variable length, meaning that
        the dimension's length is set at runtime. Represented by setting the
        length to ``"variable"``.

        :return:
            ``True`` if the dimension has variable length, False otherwise.
        :rtype: bool
        """
        return self.length == "variable"
