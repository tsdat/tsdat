from typing import Literal
from pydantic import BaseSettings


class TsdatSettings(BaseSettings):

    extra_config_properties: Literal["allow", "ignore", "forbid"] = "forbid"
