from typing import Any
from pydantic import BaseModel


class BaseRetriever(BaseModel):
    parameters: Any = {}


class SimpleRetriever(BaseRetriever):
    ...
