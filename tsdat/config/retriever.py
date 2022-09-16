import re
from typing import Dict, List, Optional, Pattern, Union, cast
from pydantic import BaseModel, Extra, Field, validator
from .utils import ParameterizedConfigClass, YamlModel

__all__ = ["RetrieverConfig"]


class DataReaderConfig(ParameterizedConfigClass):
    ...


class DataConverterConfig(ParameterizedConfigClass, extra=Extra.allow):
    ...


class RetrievedVariableConfig(BaseModel, extra=Extra.allow):
    """Specifies how the variable should be retrieved from the raw dataset and the
    preprocessing steps (i.e. DataConverters) that should be applied."""

    name: str = Field(
        description="The exact name of the variable in the raw dataset returned by the"
        " DataReader."
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


class RetrieverConfig(ParameterizedConfigClass, YamlModel, extra=Extra.allow):
    """---------------------------------------------------------------------------------
    Contains configuration parameters for the tsdat retriever class.

    This class will ultimately be converted into a tsdat.io.base.Retriever subclass for
    use in tsdat pipelines.

    Provides methods to support yaml parsing and validation, including the generation of
    json schema for immediate validation. This class also provides a method to
    instantiate a tsdat.io.base.Retriever subclass from a parsed configuration file.

    Args:
        classname (str): The dotted module path to the pipeline that the specified
            configurations should apply to. To use the built-in IngestPipeline, for
            example, you would set 'tsdat.pipeline.pipelines.IngestPipeline' as the
            classname.
        readers (Dict[str, DataReaderConfig]): The DataReaders to use for reading input
            data.

    ---------------------------------------------------------------------------------"""

    # HACK: Can't do Pattern[str]: https://github.com/samuelcolvin/pydantic/issues/2636
    readers: Optional[Dict[Pattern, DataReaderConfig]] = Field(  # type: ignore
        description="A dictionary mapping regex patterns to DataReaders that should be"
        " used to read the input data. For each input given to the Retriever, the"
        " mapping will be used to determine which DataReader to use. The patterns will"
        " be searched in the order they are defined and the DataReader corresponding"
        " with the first pattern that matches the input key will be used."
    )
    coords: Dict[str, Union[Dict[Pattern, RetrievedVariableConfig], RetrievedVariableConfig]] = Field(  # type: ignore
        {},
        description="A dictionary mapping output coordinate variable names to the"
        " retrieval rules and preprocessing actions (i.e. DataConverters) that should"
        " be applied to each retrieved coordinate variable.",
    )
    data_vars: Dict[str, Union[Dict[Pattern, RetrievedVariableConfig], RetrievedVariableConfig]] = Field(  # type: ignore
        {},
        description="A dictionary mapping output data_variable variable names to the"
        " retrieval rules and preprocessing actions (i.e. DataConverters) that should"
        " be applied to each retrieved coordinate variable.",
    )

    @validator("coords", "data_vars")
    @classmethod
    def coerce_to_patterned_retriever(cls, var_dict: Dict[str, Union[Dict[Pattern, RetrievedVariableConfig], RetrievedVariableConfig]]) -> Dict[str, Dict[Pattern[str], RetrievedVariableConfig]]:  # type: ignore
        to_return: Dict[str, Dict[Pattern[str], RetrievedVariableConfig]] = {}  # type: ignore
        for name, var_retriever in var_dict.items():  # type: ignore

            if isinstance(var_retriever, RetrievedVariableConfig):
                var_retriever = {re.compile(r".*"): var_retriever}
            to_return[name] = cast(
                Dict[Pattern[str], RetrievedVariableConfig], var_retriever
            )
        return to_return
