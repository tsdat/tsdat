from datetime import datetime
from typing import Callable, Dict, Union

import numpy as np
import pandas as pd


def datetime_substitutions(
    time: Union[datetime, np.datetime64, None],
) -> Dict[str, Callable[[], str]]:
    substitutions: Dict[str, Callable[[], str]] = {}
    if time is not None:
        t = pd.to_datetime(time)
        substitutions.update(
            year=lambda: t.strftime("%Y"),
            month=lambda: t.strftime("%m"),
            day=lambda: t.strftime("%d"),
            hour=lambda: t.strftime("%H"),
            minute=lambda: t.strftime("%M"),
            second=lambda: t.strftime("%S"),
            yyyy=lambda: t.strftime("%Y"),
            mm=lambda: t.strftime("%m"),
            dd=lambda: t.strftime("%d"),
            HH=lambda: t.strftime("%H"),
            MM=lambda: t.strftime("%M"),
            SS=lambda: t.strftime("%S"),
            date_time=lambda: t.strftime("%Y%m%d.%H%M%S"),
            date=lambda: t.strftime("%Y%m%d"),
            time=lambda: t.strftime("%H%M%S"),
            start_date=lambda: t.strftime(
                "%Y%m%d"
            ),  # included for backwards compatibility
            start_time=lambda: t.strftime(
                "%H%M%S"
            ),  # included for backwards compatibility
        )
    return substitutions
