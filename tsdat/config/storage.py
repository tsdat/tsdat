from pydantic import Field, validator
from pydantic.fields import ModelField
from typing import Dict
from .utils import ParametrizedClass, YamlModel


class DataWriterConfig(ParametrizedClass):
    """Class used to identify a `tsdat.io.writers.DataWriter` object to use for writing
    data to the storage area. Consists of a 'classname' and an optional 'parameters'
    dictionary that is given to the selected DataWriter during instantiation."""


class StorageConfig(ParametrizedClass, YamlModel):
    writers: Dict[str, DataWriterConfig] = Field(
        # min_items=1, # Doesn't work for dictionaries
        title="Output Data Writers",
        description="Register DataWriters(s) that will be used to write output data. It"
        " is left to the storage class to dispatch the DataWriter(s) on the dataset(s)"
        " to store. The built-in storage classes calls all DataWriters on each dataset",
    )

    @validator("writers")
    @classmethod
    def validate_unique_handler_names(
        cls, v: Dict[str, DataWriterConfig], field: ModelField
    ) -> Dict[str, DataWriterConfig]:
        # TODO
        # if duplicates := find_duplicates():
        #     raise ValueError(
        #         f"{field.name} contains handlers with duplicate names: {duplicates}"
        #     )
        return v
