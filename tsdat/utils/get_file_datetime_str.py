import logging
import re
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


def get_file_datetime_str(file: Union[Path, str]) -> str:
    datetime_match = re.match(r".*(\d{8}\.\d{6}).*", Path(file).name)
    if datetime_match is not None:
        return datetime_match.groups()[0]
    logger.error(f"File {file} does not contain a recognized date string")
    return ""
