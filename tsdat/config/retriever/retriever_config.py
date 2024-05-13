import re
from typing import Dict, Optional, Pattern, Union, cast
from pydantic import Extra, Field, validator
from ..utils import ParameterizedConfigClass, YamlModel

from .data_reader_config import DataReaderConfig
from .retrieved_variable_config import RetrievedVariableConfig


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
    def coerce_to_patterned_retriever(
        cls,
        var_dict: Dict[
            str, Union[Dict[Pattern, RetrievedVariableConfig], RetrievedVariableConfig]
        ],
    ) -> Dict[str, Dict[Pattern[str], RetrievedVariableConfig]]:  # type: ignore
        to_return: Dict[str, Dict[Pattern[str], RetrievedVariableConfig]] = {}  # type: ignore
        for name, var_retriever in var_dict.items():  # type: ignore
            if isinstance(var_retriever, RetrievedVariableConfig):
                var_retriever = {re.compile(r".*"): var_retriever}
            to_return[name] = cast(
                Dict[Pattern[str], RetrievedVariableConfig], var_retriever
            )
        return to_return
