"""Registry of alias for common variables that may be used in other templates."""

TEMPLATE_REGISTRY = dict(
    datastream="{location_id}.{dataset_name}[-{qualifier}][-{temporal}].{data_level}",
)

KNOWN_REGEX_PATTERNS = dict(
    year=r"(?P<year>[0-9]{4})",
    month=r"(?P<month>[0-9]{2})",
    day=r"(?P<day>[0-9]{2})",
    hour=r"(?P<hour>[0-9]{2})",
    minute=r"(?P<minute>[0-9]{2})",
    second=r"(?P<second>[0-9]{2})",
    yyyy=r"(?P<yyyy>[0-9]{4})",
    mm=r"(?P<mm>[0-9]{2})",
    dd=r"(?P<dd>[0-9]{2})",
    HH=r"(?P<HH>[0-9]{2})",
    MM=r"(?P<MM>[0-9]{2})",
    SS=r"(?P<SS>[0-9]{2})",
    date_time=r"(?P<date_time>[0-9]{8}\.[0-9]{6})",
    date=r"(?P<date>[0-9]{8})",
    time=r"(?P<time>[0-9]{6})",
    start_date=r"(?P<start_date>[0-9]{8})",
    start_time=r"(?P<start_time>[0-9]{6})",
    title=r"(?P<title>[a-zA-Z0-9_]+)",  # plot title
    location_id=r"(?P<location_id>[a-zA-Z0-9_]+)",  # from tsdat.config.attributes
    dataset_name=r"(?P<dataset_name>[a-z0-9_]+)",  # from tsdat.config.attributes
    qualifier=r"(?P<qualifier>[a-zA-Z0-9_]+)",  # from tsdat.config.attributes
    temporal=r"(?P<temporal>[0-9]+[a-zA-Z]+)",  # from tsdat.config.attributes
    data_level=r"(?P<data_level>[a-z0-9]+)",  # from tsdat.config.attributes
)

UNKNOWN_REGEX_PATTERN = "(?P<{variable}>[a-zA-Z0-9_]+)"


def get_regex(var_name: str) -> str:
    return KNOWN_REGEX_PATTERNS.get(
        var_name,
        UNKNOWN_REGEX_PATTERN.format(variable=var_name),
    )
