# TODO: Add UnitsConverter
# TODO: Add StringTimeConverter
# TODO: Determine mechanism for dispatching converters

from abc import ABC, abstractmethod
from typing import Any, Dict
from pydantic import BaseModel, Extra


# TODO: Noticing a lot of classes like:
# BaseConverter(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
# BaseStorage(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
#   ...
# BasePipeline(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
#   ...
# BaseDataHandler(BaseModel, ABC, extra=Extra.forbid):
#   parameters: Dict[str, Any] = {}
#   ...
# and so on... might be about time to standardize these via object inheritance


class BaseConverter(BaseModel, ABC, extra=Extra.forbid):
    parameters: Dict[str, Any] = {}

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any):
        ...

    # TODO: Think about refactoring the converter interface; previously these defined a
    # run method with signature (data, in_units, out_units). This worked for the units
    # converter (known as DefaultConverter), but in_units/out_units not used for the
    # StringTimeConverter, nor the TimestampConverter (which, incidentally, is also
    # never used). Seems like the old version basically just used the VariableDefinition
    # to scrape the in/out units and then ran the converters with those. Why not just
    # pass the VariableDefinition (Now VariableConfig) itself and the data, and let the
    # converters figure out what to do with it.
