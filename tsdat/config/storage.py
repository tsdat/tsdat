from pydantic import Field
from .utils import ParametrizedConfigClass, YamlModel


class DataHandlerConfig(ParametrizedConfigClass):
    ...


class StorageConfig(ParametrizedConfigClass, YamlModel):
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
