import logging
from datetime import datetime
from pathlib import Path
from typing import Union

from tsdat.tstring import Template

logger = logging.getLogger(__name__)


def get_file_datetime(file: Union[Path, str], filename_template: str) -> datetime:
    template = Template(filename_template)
    properties: dict[str, str] = template.extract_substitutions(str(file)) or {}

    year_fields = dict(
        year=lambda prop: prop,
        yyyy=lambda prop: prop,
        date_time=lambda prop: prop[:4],
        start_date=lambda prop: prop[:4],
        date=lambda prop: prop[:4],
    )
    month_fields = dict(
        month=lambda prop: prop,
        mm=lambda prop: prop,
        date_time=lambda prop: prop[4:6],
        start_date=lambda prop: prop[4:6],
        date=lambda prop: prop[4:6],
    )
    day_fields = dict(
        day=lambda prop: prop,
        dd=lambda prop: prop,
        date_time=lambda prop: prop[6:8],
        start_date=lambda prop: prop[6:8],
        date=lambda prop: prop[6:8],
    )
    hour_fields = dict(
        hour=lambda prop: prop,
        HH=lambda prop: prop,
        date_time=lambda prop: prop.split(".")[1][:2],
        start_time=lambda prop: prop[:2],
        time=lambda prop: prop[:2],
    )
    minute_fields = dict(
        minute=lambda prop: prop,
        MM=lambda prop: prop,
        date_time=lambda prop: prop.split(".")[1][2:4],
        start_time=lambda prop: prop[2:4],
        time=lambda prop: prop[2:4],
    )
    second_fields = dict(
        second=lambda prop: prop,
        SS=lambda prop: prop,
        date_time=lambda prop: prop.split(".")[1][4:6],
        start_time=lambda prop: prop[4:6],
        time=lambda prop: prop[4:6],
    )

    year: int | None = None
    month: int | None = None
    day: int | None = None
    hour: int | None = None
    minute: int | None = None
    second: int | None = None

    for field, func in year_fields.items():
        if field in properties:
            year = int(func(properties[field]))
            break

    for field, func in month_fields.items():
        if field in properties:
            month = int(func(properties[field]))
            break
    for field, func in day_fields.items():
        if field in properties:
            day = int(func(properties[field]))
            break
    for field, func in hour_fields.items():
        if field in properties:
            hour = int(func(properties[field]))
            break
    for field, func in minute_fields.items():
        if field in properties:
            minute = int(func(properties[field]))
            break
    for field, func in second_fields.items():
        if field in properties:
            second = int(func(properties[field]))
            break

    return datetime(
        year=year or 1,
        month=month or 1,
        day=day or 1,
        hour=hour or 0,
        minute=minute or 0,
        second=second or 0,
    )
