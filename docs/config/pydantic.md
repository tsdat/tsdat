# Pydantic and Parameters

Tsdat makes use of [Pydantic](https://pydantic-docs.helpmanual.io/) to support automatic validation of the yaml
configuration files. This means that each customizable class type in Tsdat that can be specified in a yaml file extends
a Pydantic base model object. To facilitate consistent format and validation checking of parameters, all Tsdat objects
should include a `Parameters` inner class inheriting from `pydantic.BaseModel`. This means that if you create a custom
object (such as a custom `DataReader`) then your class should include a `Parameters` inner class as shown in the
following example:

```python
import xarray as xr
from tsdat import DataReader
from pydantic import BaseModel, Extra


class CustomDataReader(DataReader):
    """Data reader that can read from *xyz* formatted-data files."""

    class Parameters(BaseModel, extra=Extra.forbid):
        """If your CustomDataReader should take any additional arguments from the
        retriever config configuration file, then those should be specified here.
        """
        custom_parameter: bool = True  # <- Use Python type hinting to validate the type when loaded from config

    parameters: Parameters = Parameters()

    def read(self, input_key: str) -> xr.Dataset:
        return xr.Dataset()
```

The following object types all support custom parameters via the same mechanism shown above:

- `tsdat.DataReader`
- `tsdat.DataWriter`
- `tsdat.DataHandler`
- `tsdat.DataConverter`
- `tsdat.QualityChecker`
- `tsdat.QualityHandler`
- `tsdat.Storage`
- `tsdat.Retriever`
