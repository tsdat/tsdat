"""-------------------------------------------------------------------------------------
This module contains custom DataConverter classes that can be used when retrieving input
data from raw data files to convert data values to the format required by the output
dataset.yaml definition.

-------------------------------------------------------------------------------------"""

import xarray as xr
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict
from tsdat import DataConverter, DatasetConfig, RetrievedDataset


# DEVELOPER: Implement your custom DataConverter, giving it a better name and
# documentation as you do so.
class CustomDataConverter(DataConverter):
    """---------------------------------------------------------------------------------
    Custom DataConverter that can be used to preprocess input datasets and convert them
    into a suitable format for downstream processing.

    Built-in examples of DataConverters include the UnitsConverter to convert units
    (e.g., degF -> degC) and the StringToDatetime converter to convert time variables
    encoded as strings into np.datetime64 objects.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel):
        model_config = ConfigDict(extra="forbid")
        """If your CustomConverter should take any additional arguments from the
        retriever configuration file, then those should be specified here.

        e.g.,:
        custom_parameter: float = 5.0

        """

    parameters: Parameters = Parameters()
    """Extra parameters that can be set via the retrieval configuration file. If you opt
    to not use any configuration parameters then please remove the code above."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: DatasetConfig,
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        """----------------------------------------------------------------------------
        Applies a custom conversion to the retrieved data.

        Args:
            data (xr.DataArray): The DataArray corresponding with the retrieved data
                variable to convert.
            variable_name (str): The name of the variable to convert.
            dataset_config (DatasetConfig): The output dataset configuration.
            retrieved_dataset (RetrievedDataset): The retrieved dataset structure.

        Returns:
            Optional[xr.DataArray]: The converted data as an xr.DataArray, or None if
                the conversion was done in place.

        ----------------------------------------------------------------------------"""
        raise NotImplementedError
