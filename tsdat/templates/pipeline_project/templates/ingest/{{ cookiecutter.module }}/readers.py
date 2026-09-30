from typing import Dict, Union
from pydantic import BaseModel, ConfigDict
import xarray as xr
from tsdat import DataReader


# DEVELOPER: Implement or remove the CustomDataReader. If implementing it, please
# rename it to appropriately reflect the type of data it is reading.
class CustomDataReader(DataReader):
    """---------------------------------------------------------------------------------
    Custom DataReader that can be used to read data from a specific format.

    Built-in implementations of data readers can be found in the
    [tsdat.io.readers](https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/io/readers)
    module.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel):
        model_config = ConfigDict(extra="forbid")
        """If your CustomDataReader should take any additional arguments from the
        retriever configuration file, then those should be specified here.

        e.g.,:
        custom_parameter: float = 5.0

        """

    parameters: Parameters = Parameters()
    """Extra parameters that can be set via the retrieval configuration file. If you opt
    to not use any configuration parameters then please remove the code above."""

    def read(self, input_key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        # DEVELOPER: Implement the read method. This takes an input_key which is the path
        # to the file being run. It should open the file and return either a single
        # Dataset object, or a mapping of strings to Datasets.
        raise NotImplementedError
        return xr.Dataset()
