from pydantic import Extra

from ..utils import ParameterizedConfigClass


class DataConverterConfig(ParameterizedConfigClass, extra=Extra.allow): ...
