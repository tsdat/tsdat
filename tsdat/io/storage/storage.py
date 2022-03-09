import xarray as xr
from abc import ABC, abstractmethod
from typing import Any, Dict
from pydantic import BaseModel, Extra
from tsdat.io.handlers.handlers import HandlerRegistry


class BaseStorage(BaseModel, ABC, extra=Extra.forbid):

    parameters: Dict[str, Any] = {}
    registry: HandlerRegistry

    @abstractmethod
    def save(self, dataset: xr.Dataset):
        # self.registry.write(dataset) could work?
        ...
