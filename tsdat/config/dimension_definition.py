class DimKeys:
    LENGTH = 'length'


class DimensionDefinition:
    def __init__(self, name: str, length):
        self.name: str = name
        self.length: str = length.get(DimKeys.LENGTH, None)

    def is_unlimited(self) -> bool:
        """Returns True is the dimension has unlimited length. Represented by
        setting the length attribute to "unlimited".

        Returns:
            bool: True if the dimension has unlimited length.
        """
        return self.length == "unlimited"
    
    def is_variable_length(self) -> bool:
        """-------------------------------------------------------------------
        Returns True if the dimension has variable length, meaning that the
        dimension's length is set at runtime. Represented by setting the 
        length to "variable".

        Returns:
            bool: True if the dimension has variable length.
        -------------------------------------------------------------------"""
        return self.length == "variable"
