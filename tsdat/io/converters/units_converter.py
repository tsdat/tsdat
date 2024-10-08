import logging
from typing import Any, Optional

import pandas as pd
import xarray as xr

from tsdat.config.variables.ureg import check_unit, ureg

from ...config.dataset import DatasetConfig
from ..base import DataConverter, RetrievedDataset

logger = logging.getLogger(__name__)


class UnitsConverter(DataConverter):
    """Converts the units of a retrieved variable to specified output units.

    If the 'input_units' property is set then that string is used to determine the input
    input units, otherwise the converter will attempt to look up and use the 'units'
    attribute on the specified variable in the dataset provided to the `convert` method.
    If the input units cannot be set then a warning is issued and the original dataset
    is returned. The output units are specified by the output dataset configuration."""

    # IDEA: Validate the literal value of the input units string according to pint
    input_units: Optional[str] = None
    """The units of the input data."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: DatasetConfig,
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        # Can only convert number-like dtypes (not strings, datetimes, objects)
        if not pd.api.types.is_any_real_numeric_dtype(data.data):
            return None

        # Get output units and convert udunits for pint if need be
        output_units = dataset_config[variable_name].attrs.units
        if output_units is None or output_units == "1":
            return None
        output_units = check_unit(output_units, keep_exp=True)

        # Get input units and convert udunits for pint if need be
        input_units = self._get_input_units(data)
        if input_units == "1":
            return None
        input_units = check_unit(input_units, keep_exp=True)

        out_dtype = dataset_config[variable_name].dtype
        if input_units == output_units:
            if not output_units:
                logger.warning(
                    "Output units for variable '%s' could not be found. Please ensure"
                    " these are set in the dataset configuration file for the specified"
                    " variable.",
                    variable_name,
                )
            return None

        # Run pint and set output data
        converted = (data.data * ureg(input_units)).to(output_units).magnitude
        data_array = data.copy(data=converted.astype(out_dtype))

        # Use original output units text
        data_array.attrs["units"] = dataset_config[variable_name].attrs.units
        logger.debug(
            "Converted '%s's units from '%s' to '%s'",
            variable_name,
            input_units,
            output_units,
        )
        return data_array

    def _get_input_units(self, data: xr.DataArray) -> str:
        units = "1"
        if self.input_units:
            units = self.input_units
        elif "units" in data.attrs:
            units = data.attrs["units"]
        return units
