import re
from pydantic import BaseModel, Field, validator
from pydantic.fields import ModelField
from typing import List, Pattern
from .utils import ParametrizedClass, YamlModel, find_duplicates


class DataHandlerConfig(ParametrizedClass):
    name: str = Field(
        description="A label used internally to distinguish registered DataHandlers."
    )


class DataReaderConfig(DataHandlerConfig):
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


class DataWriterConfig(DataHandlerConfig):
    ...


class HandlerRegistryConfig(BaseModel):
    # TODO: rename to readers and writers
    input_handlers: List[DataReaderConfig] = Field(
        min_items=1,
        title="Input Data Handlers",
        description="Register a list of DataHandler(s) that will be used to read input"
        " data. If multiple input DataHandlers are used, then you must add a 'regex'"
        " attribute for each that will be used to map input keys (i.e. file paths) to"
        " the input DataHandler that should be used to read that resource.",
    )
    output_handlers: List[DataWriterConfig] = Field(
        min_items=1,
        title="Output Data Handlers",
        description="Register a list of DataHandler(s) that will be used to write"
        " output data. If multiple output DataHandlers are used, then you must add a"
        " 'regex' attribute for each that will be used to map output keys (i.e. file"
        " paths) to the DataHandler that should be used to write that resource.",
    )

    @validator("input_handlers", "output_handlers")
    @classmethod
    def validate_unique_handler_names(
        cls, v: List[DataHandlerConfig], field: ModelField
    ) -> List[DataHandlerConfig]:
        if duplicates := find_duplicates(v):
            raise ValueError(
                f"{field.name} contains handlers with duplicate names: {duplicates}"
            )
        return v

    @validator("input_handlers")
    @classmethod
    def validate_regex_patterns(
        cls, v: List[DataHandlerConfig], field: ModelField
    ) -> List[DataHandlerConfig]:
        if len(v) == 1:
            if not v[0].regex:  # type: ignore
                v[0].regex = re.compile(r".*")  # type: ignore

        # Ensure handlers define regex patterns if len > 1.
        elif any(not dh.regex for dh in v):  # type: ignore
            raise ValueError(
                f"If len({field.name}) > 1 then all handlers should define a 'regex'"
                " pattern."
            )
        return v


class StorageConfig(ParametrizedClass, YamlModel):
    registry: HandlerRegistryConfig = Field(
        title="Handler Registry",
        description="Register lists of DataReader(s) and DataWriter(s) to be used to"
        " read and write data from the pipeline.",
    )
