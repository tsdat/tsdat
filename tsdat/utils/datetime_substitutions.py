from datetime import datetime
from typing import Dict, Union

import numpy as np
import pandas as pd


def datetime_substitutions(
    time: Union[datetime, np.datetime64, None],
) -> Dict[str, str]:
    substitutions: Dict[str, str] = {}
    if time is not None:
        t = pd.to_datetime(time)
        substitutions.update(
            year=t.strftime("%Y"),
            month=t.strftime("%m"),
            day=t.strftime("%d"),
            hour=t.strftime("%H"),
            minute=t.strftime("%M"),
            second=t.strftime("%S"),
            yyyy=t.strftime("%Y"),
            mm=t.strftime("%m"),
            dd=t.strftime("%d"),
            HH=t.strftime("%H"),
            MM=t.strftime("%M"),
            SS=t.strftime("%S"),
            date_time=t.strftime("%Y%m%d.%H%M%S"),
            date=t.strftime("%Y%m%d"),
            time=t.strftime("%H%M%S"),
            start_date=t.strftime("%Y%m%d"),  # included for backwards compatibility
            start_time=t.strftime("%H%M%S"),  # included for backwards compatibility
        )
    return substitutions
