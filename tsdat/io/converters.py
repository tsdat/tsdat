# TODO: Implement UnitsConverter
# TODO: Implement StringToDatetime
# IDEA: Implement MultiDimensionalGrouper (better name needed. goes from collection of 1D
# variables to one 2D variable)

import xarray as xr

from abc import ABC, abstractmethod
from tsdat.config.dataset import DatasetConfig
from tsdat.config.retrieval import RetrieverConfig
from tsdat.utils import ParametrizedClass


# class UnitsConverter(BaseDataConverter):
#     def run(self, dataset: xr.Dataset, dataset_config: DatasetConfig) -> xr.Dataset:
#         if (
#             variable_config.input is None
#             or variable_config.input.units is None
#             or variable_config.attrs.units is None
#             or variable_config.input.units == "1"
#             or variable_config.attrs.units == "1"
#             or variable_config.input.units == variable_config.attrs.units
#         ):
#             return data
#         return act.utils.data_utils.convert_units(  # type: ignore
#             data=data,
#             in_units=variable_config.input.units,
#             out_units=variable_config.attrs.units,
#         )


# class StringTimeConverterParameters(BaseModel, extra=Extra.forbid):
#     time_format: str
#     timezone: Optional[str] = "UTC"
#     dtype: str = "datetime64[ns]"


# class StringTimeConverter(BaseDataConverter):

#     parameters: StringTimeConverterParameters

#     def run(self, dataset: xr.Dataset, dataset_config: DatasetConfig) -> xr.Dataset:
#         dt = pd.to_datetime(data, format=self.parameters.time_format)  # type: ignore

#         if self.parameters.timezone:
#             dt = dt.tz_localize(self.parameters.timezone).tz_convert("UTC")  # type: ignore
#             # HACK: numpy can't handle localized datetime arrays so we remove the
#             # timezone from the datetime array after converting it to UTC
#             dt = dt.tz_localize(None)  # type: ignore

#         return np.array(dt, dtype=self.parameters.np_dtype)  # type: ignore
