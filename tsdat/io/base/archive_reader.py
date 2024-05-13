from abc import ABC
from typing import (
    Dict,
)

from .data_reader import DataReader


class ArchiveReader(DataReader, ABC):
    """------------------------------------------------------------------------------------
    Base class for DataReader objects that read data from archives.
    Subclasses of `ArchiveHandler` may define additional parameters to support various
    methods of unpacking archived data.

    ------------------------------------------------------------------------------------
    """

    exclude: str = ""

    def __init__(self, parameters: Dict = None):  # type: ignore
        super().__init__(parameters=parameters)

        # Naively merge a list of regex patterns to exclude certain files from being
        # read. By default we exclude files that macOS creates when zipping a folder.
        exclude = [".*\\_\\_MACOSX/.*", ".*\\.DS_Store"]
        exclude.extend(getattr(self.parameters, "exclude", []))
        self.parameters.exclude = "(?:% s)" % "|".join(exclude)
