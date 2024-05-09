from typing import List, Union
from pydantic import BaseModel, Extra, Field

from .data_converter_config import DataConverterConfig


class RetrievedVariableConfig(BaseModel, extra=Extra.allow):
    """Specifies how the variable should be retrieved from the raw dataset and the
    preprocessing steps (i.e. DataConverters) that should be applied."""

    name: Union[str, List[str]] = Field(
        description="The exact name or list of names of the variable in the raw dataset"
        " returned by the DataReader."
    )
    data_converters: List[DataConverterConfig] = Field(
        [],
        description="A list of DataConverters to run for this variable. Common choices"
        " include the tsdat UnitsConverter (classname: "
        "'tsdat.io.converters.UnitsConverter') to convert the variable's data from its"
        " input units to specified output units, and the tsdat StringToDatetime"
        " converter (classname: 'tsdat.io.converters.StringToDatetime'), which takes"
        " dates/times formatted as strings and converts them into a datetime64 object"
        " that can be used throughout the rest of the pipeline. This property is"
        " optional and defaults to [].",
    )
