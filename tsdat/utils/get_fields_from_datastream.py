from typing import Dict

from tsdat.const import DATASTREAM_TEMPLATE


def get_fields_from_datastream(datastream: str) -> Dict[str, str]:
    """Extracts fields from the datastream.

    WARNING: this only works for the default datastream template.
    """
    fields = DATASTREAM_TEMPLATE.extract_substitutions(datastream)
    if fields is None:
        return {}
    return {k: v for k, v in fields.items() if v is not None}
