import logging
from typing import Any, Optional

import act  # type: ignore
import xarray as xr
from numpy.typing import NDArray

from ..base import DataConverter, RetrievedDataset
from ...config.dataset import DatasetConfig

logger = logging.getLogger(__name__)


class UnitsConverter(DataConverter):
    """---------------------------------------------------------------------------------
    Converts the units of a retrieved variable to specified output units.

    If the 'input_units' property is set then that string is used to determine the input
    input units, otherwise the converter will attempt to look up and use the 'units'
    attribute on the specified variable in the dataset provided to the `convert` method.
    If the input units cannot be set then a warning is issued and the original dataset
    is returned. The output units are specified by the output dataset configuration.

    Args:
        input_units (Optional[str]): The units that the retrieved data comes in.

    ---------------------------------------------------------------------------------"""

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
        input_units = self._get_input_units(data)
        if not input_units:
            logger.warning(
                "Input units for variable '%s' could not be found. Please ensure these"
                " are set either in the retrieval configuration file, or are set on the"
                " 'units' attribute of the '%s' variable in the dataset to converted.",
                variable_name,
                variable_name,
            )
            return None

        output_units = dataset_config[variable_name].attrs.units
        if (
                not output_units
                or output_units == "1"
                or output_units == "unitless"
                or input_units == output_units
        ):
            if not output_units:
                logger.warning(
                    "Output units for variable %s could not be found. Please ensure"
                    " these are set in the dataset configuration file for the specified"
                    " variable.",
                    variable_name,
                )
            return None

        converted: NDArray[Any] = act.utils.data_utils.convert_units(  # type: ignore
            data=data.data,
            in_units=input_units,
            out_units=output_units,
        )
        data_array = data.copy(data=converted)
        data_array.attrs["units"] = output_units
        logger.debug(
            "Converted '%s's units from '%s' to '%s'",
            variable_name,
            input_units,
            output_units,
        )
        return data_array

    def _get_input_units(self, data: xr.DataArray) -> str:
        units = ""
        if self.input_units:
            units = self.input_units
        elif "units" in data.attrs:
            units = data.attrs["units"]
        return units
