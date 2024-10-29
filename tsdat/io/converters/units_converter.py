import logging
from typing import Any, Optional

import pandas as pd
import xarray as xr
from pint.errors import PintError

from tsdat.config.variables.ureg import check_unit, ureg

from ...config.dataset import DatasetConfig
from ...config.variables import Variable
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

        output_units = self._get_output_units(dataset_config[variable_name])
        if output_units is None:
            return None

        input_units = self._get_input_units(data, variable_name)
        if (
            (not input_units)
            or (input_units == "1")
            or (output_units == "1")
            or (input_units == output_units)
        ):
            data_array = data.copy().astype(dataset_config[variable_name].dtype)
            data_array.attrs["units"] = dataset_config[variable_name].attrs.units
            return data_array

        try:
            # Run pint and set output data
            out_dtype = dataset_config[variable_name].dtype
            converted = (data.data * ureg(input_units)).to(output_units).magnitude
            data_array = data.copy(data=converted.astype(out_dtype))
        except AttributeError:
            print(f"the what??? {data=}, {ureg(input_units)=}, {output_units=}")
            raise

        # Use original output units text
        data_array.attrs["units"] = dataset_config[variable_name].attrs.units
        logger.debug(
            "Converted '%s's units from '%s' to '%s'",
            variable_name,
            input_units,
            output_units,
        )
        return data_array

    def _get_input_units(
        self, data: xr.DataArray | None, variable_name: str
    ) -> str | None:
        input_units = None
        if self.input_units:
            input_units = self.input_units.strip()
        elif data is not None and "units" in data.attrs:
            input_units = data.attrs["units"].strip()

        if input_units is None or not input_units:
            return None

        try:
            input_units = check_unit(input_units, keep_exp=True)
        except PintError:
            logger.warning(
                "Input units for variable '%s' are invalid ('%s'). Please ensure these"
                " are set in the retrieval configuration file for the specified"
                " variable.",
                variable_name,
                input_units,
            )
            return None
        return input_units

    def _get_output_units(self, var_config: Variable) -> str | None:
        # Get output units and convert udunits for pint if need be
        output_units = var_config.attrs.units or ""
        try:
            output_units = check_unit(output_units, keep_exp=True)
        except PintError:
            logger.warning(
                "Output units for variable '%s' could not be found. Please ensure these"
                " are set in the dataset configuration file for the specified"
                " variable.",
                var_config.name,
            )
            return None
        return output_units
