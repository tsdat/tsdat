from pydantic import Field

from ..utils import ParameterizedConfigClass, YamlModel
from .data_handler_config import DataHandlerConfig


class StorageConfig(ParameterizedConfigClass, YamlModel):
    """Contains configuration parameters for the data storage API used in tsdat pipelines.

    This class will ultimately be converted into a tsdat.io.base.Storage subclass for
    use in tsdat pipelines.

    Provides methods to support yaml parsing and validation, including the generation of
    json schema for immediate validation. This class also provides a method to
    instantiate a tsdat.io.base.Storage subclass from a parsed configuration file."""

    handler: DataHandlerConfig = Field(
        DataHandlerConfig(classname="tsdat.io.handlers.NetCDFHandler", parameters={}),
        title="Output Data Handler",
        description="Register a DataHandler for the Storage class to use for reading"
        " from and writing to the storage area. For most users, the default DataHandler"
        " ('tsdat.io.handlers.NetCDFHandler') is sufficient. Tsdat strongly encourages"
        " using the default NetCDFHandler because it is the most well-supported format"
        " offered out-of-the-box. Other formats are provided, and custom formats can"
        " also be added to extend the default functionality of tsdat. Note that some"
        " Storage classes may not support certain DataHandlers (e.g., Storage classes"
        " targeted at Databases may not support file-based DataHandlers).",
    )
    """Config class that should be used for data I/O within the storage area."""
