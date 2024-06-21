from tsdat.const import DATASTREAM_TEMPLATE


def get_datastream(**global_attrs: str) -> str:
    return DATASTREAM_TEMPLATE.substitute(global_attrs)
