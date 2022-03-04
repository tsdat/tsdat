from io import BytesIO
import xarray as xr
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional, Union, Pattern
from pydantic import BaseModel


class BaseDataHandler(BaseModel):
    name: str
    regex: Pattern[str]
    parameters: Dict[str, Any] = {}


class DataReader(BaseDataHandler, ABC):
    @abstractmethod
    def read(
        self, key: Union[str, BytesIO], name: Optional[str] = None
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...


class DataWriter(BaseDataHandler, ABC):
    @abstractmethod
    def write(self, ds: xr.Dataset, key: Optional[str] = None):
        ...


class HandlerRegistry(BaseModel):
    # TODO: Implement the methods on this class
    # Because duplicate patterns are allowed, each registry is represented using a list
    # of DataHandlers. Matches are assessed by iterating through all input/output
    # handlers and matching the input key to each regex pattern.
    input_handlers: List[DataReader]
    output_handlers: List[DataWriter]

    def register_input_handler(self, handler: DataReader):
        if handler in self.input_handlers:
            raise ValueError(
                f"InputHandler '{handler.name}' cannot be registered twice."
            )
        self.input_handlers.append(handler)

    def register_output_handler(self, handler: DataWriter):
        if handler in self.output_handlers:
            raise ValueError(
                f"OutputHandler '{handler.name}' cannot be registered twice."
            )
        self.output_handlers.append(handler)

    def match_input_key(self, key: str) -> List[DataReader]:
        ...

    def match_output_key(self, key: str) -> List[DataWriter]:
        ...

    def read(self, key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        ...

    def write(self, dataset: xr.Dataset):
        ...
