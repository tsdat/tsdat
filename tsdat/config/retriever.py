import re
from typing import Dict, Pattern
from pydantic import Field, validator, Extra
from pydantic.fields import ModelField
from .utils import ParametrizedConfigClass, YamlModel

__all__ = ["RetrieverConfig"]


class DataReaderConfig(ParametrizedConfigClass):
    # HACK: Can't do Pattern[str]: https://github.com/samuelcolvin/pydantic/issues/2636
    regex: Pattern = Field(  # type: ignore
        "",
        description="A regex pattern used to map input data keys (e.g., a file path or"
        " url passed as input from the pipeline runner) to the DataReader that should"
        " be used to read that resource. If there are multiple DataReader registered"
        " and an input data key would be matched by the regex pattern of multiple"
        " DataReaders, then only the DataReader corresponding with the first match will"
        " be used. Because of this, we recommend ordering your DataReaders from most"
        " specific pattern to least specific so that the most specific pattern will be"
        " matched first.",
    )


class RetrieverConfig(ParametrizedConfigClass, YamlModel, extra=Extra.allow):
    """---------------------------------------------------------------------------------
    Class used to contain configuration parameters for the tsdat retriever class. This
    class will ultimately be converted into a tsdat.io.base.Retriever subclass for use
    in tsdat pipelines.

    Provides methods to support yaml parsing and validation, including the generation of
    json schema for immediate validation. This class also provides a method to
    instantiate a tsdat.io.base.Retriever subclass from a parsed configuration file.

    Args:
        classname (str): The dotted module path to the pipeline that the specified
        configurations should apply to. To use the built-in IngestPipeline, for example,
        you would set 'tsdat.pipeline.pipelines.IngestPipeline' as the classname.
        readers (Dict[str, DataReaderConfig]): The DataReaders to use for reading input
        data.

    ---------------------------------------------------------------------------------"""

    readers: Dict[str, DataReaderConfig]

    @validator("readers")
    @classmethod
    def validate_regex_patterns(
        cls, readers: Dict[str, DataReaderConfig], field: ModelField
    ) -> Dict[str, DataReaderConfig]:

        if len(readers) == 1:
            reader = list(readers.values())[0]

            if not reader.regex:  # type: ignore
                reader.regex = re.compile(r".*")  # type: ignore

        # Ensure handlers define regex patterns if len > 1.
        elif any(not dh.regex for dh in readers.values()):  # type: ignore
            raise ValueError(
                f"If len({field.name}) > 1 then all readers should define a 'regex'"
                " pattern."
            )
        return readers
