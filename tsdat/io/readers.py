# TODO: Implement NetCDFReader
# TODO: Implement CSVReader
# TODO: Implement ZipReader
# TODO: Implement ZarrReader
# TODO: Implement ParquetReader

import xarray as xr
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Dict, Optional, Union
from tsdat.utils import ParametrizedClass
