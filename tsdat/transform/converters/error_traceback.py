import warnings
import logging

logger = logging.getLogger(__name__)


def error_traceback(error):
    warnings.warn(
        "\n\nEncountered an error running transformer. Please ensure necessary"
        " dependencies are installed."
    )
    logger.exception(error)
