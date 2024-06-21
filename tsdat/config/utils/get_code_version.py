import logging
import os

from dunamai import Style, Version

logger = logging.getLogger(__name__)


def get_code_version() -> str:
    version = "N/A"
    try:
        version = os.environ["CODE_VERSION"]
    except KeyError:
        try:
            version = Version.from_git().serialize(dirty=True, style=Style.SemVer)
        except RuntimeError:
            logger.warning(
                "Could not get code_version from either the 'CODE_VERSION' environment"
                " variable nor from git history. The 'code_version' global attribute"
                " will be set to 'N/A'.",
            )
    return version
