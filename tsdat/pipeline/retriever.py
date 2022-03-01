from abc import ABC, abstractmethod
from typing import Any
from tsdat.config.pipeline import Config

# TODO: Implement a basic retriever (retrieval rules) class which takes a
# DatasetDefinition instance and determines how variables should be extracted and
# instantiated from an input file (i.e. for an ingest)

# If doing this, we also need an option in the pipeline config or at a minimum we need
# to default to using some basic retriever class


class Retriever(ABC):

    config: Config

    @abstractmethod
    def retrieve(self, inputs: Any) -> Any:
        ...


class BasicRetriever:
    ...
