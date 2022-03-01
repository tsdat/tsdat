import re
from pydantic import Field, StrictStr, validator
from pydantic.fields import ModelField
from typing import List, Pattern
from .utils import ParametrizedClass, YamlModel


class DataHandler(ParametrizedClass):
    name: StrictStr = Field(
        description="A label used internally to distinguish registered DataHandlers."
    )
    # HACK: Can't do Pattern[str]: https://github.com/samuelcolvin/pydantic/issues/2636
    regex: Pattern  # type: ignore


class InputDataHandler(DataHandler):
    regex: Pattern = Field(  # type: ignore
        "",
        description="A regex pattern used to map input data keys (e.g., a file path or"
        " url passed as input from the pipeline runner) to a DataHandler that should be"
        " used to read that resource. If there are multiple input DataHandlers"
        " registered, then this field is required. Note that the default pattern is"
        " '.*' if there is only one registered DataHandler, but no default is set if"
        " multiple data handlers are registered.",
    )


class OutputDataHandler(DataHandler):
    regex: Pattern = Field(  # type: ignore
        "",
        description="A regex pattern used to map output data keys (e.g., a file path or"
        " url passed from the storage class's save method) to a DataHandler that should"
        " be used to write that resource. If there are multiple output DataHandlers"
        " registered and all of them match this pattern, then by default the storage"
        " will dispatch all of them for the given key. Note that the default pattern is"
        " '.*' if there is only one registered DataHandler, but no default is set if"
        " multiple data handlers are registered.",
    )


class StorageDefinition(ParametrizedClass, YamlModel):
    input_handlers: List[InputDataHandler] = Field(
        min_items=1,
        title="Input Data Handlers",
        description="Register a list of DataHandler(s) that will be used to read input"
        " data. If multiple input DataHandlers are used, then you must add a 'regex'"
        " attribute for each that will be used to map input keys (i.e. file paths) to"
        " the input DataHandler that should be used to read that resource.",
    )
    output_handlers: List[OutputDataHandler] = Field(
        min_items=1,
        title="Output Data Handlers",
        description="Register a list of DataHandler(s) that will be used to write"
        " output data. If multiple output DataHandlers are used, then you must add a"
        " 'regex' attribute for each that will be used to map output keys (i.e. file"
        " paths) to the DataHandler that should be used to write that resource.",
    )

    @validator("input_handlers", "output_handlers")
    @classmethod
    def validate_regex_patterns(
        cls, v: List[DataHandler], field: ModelField
    ) -> List[DataHandler]:
        # Set a catch-all default value
        if len(v) == 1:
            if not v[0].regex:  # type: ignore
                v[0].regex = re.compile(r".*")  # type: ignore

        # Ensure handlers define regex patterns if len > 1
        elif any(not dh.regex for dh in v):  # type: ignore
            raise ValueError(
                f"If len({field.name}) > 1 then all handlers should define a 'regex'"
                " pattern."
            )

        return v

    # TODO: require unique InputDataHandler names, OutputDataHandler names
