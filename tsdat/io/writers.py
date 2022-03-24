# TODO: Implement NetCDFWriter
# TODO: Implement CSVWriter
# TODO: Implement ZarrWriter
# TODO: Implement ParquetWriter

from typing import Any, Dict
import xarray as xr
from pathlib import Path
from pydantic import BaseModel, Extra
from .base import FileWriter
