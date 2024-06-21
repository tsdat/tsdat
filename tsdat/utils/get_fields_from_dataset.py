from typing import Any, Dict
import xarray as xr

from .datetime_substitutions import datetime_substitutions


def get_fields_from_dataset(dataset: xr.Dataset) -> Dict[str, Any]:
    return {
        **dict(dataset.attrs),
        **datetime_substitutions(dataset.time.values[0]),
    }
