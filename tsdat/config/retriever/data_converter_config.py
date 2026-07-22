from pydantic import ConfigDict

from ..utils import ParameterizedConfigClass


class DataConverterConfig(ParameterizedConfigClass):
    model_config = ConfigDict(extra='allow')

