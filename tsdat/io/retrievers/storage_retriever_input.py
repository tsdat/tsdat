from datetime import datetime
import logging
import shlex
from typing import (
    Dict,
)

logger = logging.getLogger(__name__)


class StorageRetrieverInput:
    """Returns an object representation of an input storage key.

    Input storage keys should be formatted like:

    ```python
    "--datastream sgp.met.b0 --start 20230801 --end 20230901"
    "--datastream sgp.met.b0 --start 20230801 --end 20230901 --location_id sgp --data_level b0"
    ```
    """

    def __init__(self, input_key: str):
        kwargs: Dict[str, str] = {}

        if len(input_key.split("::")) == 3:
            logger.warning(
                "Using old Storage input key format (datastream::start::end)."
            )
            datastream, _start, _end = input_key.split("::")
            kwargs["datastream"] = datastream
            kwargs["start"] = _start
            kwargs["end"] = _end
        else:
            args = shlex.split(input_key)
            key = ""
            for arg in args:
                if arg.startswith("-"):
                    key = arg.lstrip("-")
                    kwargs[key] = ""
                elif key in kwargs:
                    kwargs[key] = arg
                    key = ""
                else:
                    raise ValueError(
                        "Bad storage retriever input key. Expected format like"
                        f" '--key1 value1 --key2 value2 ...', got '{input_key}'."
                    )

        self.input_key = input_key
        self.datastream = kwargs.pop("datastream")
        self._start = kwargs.pop("start")
        self._end = kwargs.pop("end")

        start_format = "%Y%m%d.%H%M%S" if "." in self._start else "%Y%m%d"
        end_format = "%Y%m%d.%H%M%S" if "." in self._end else "%Y%m%d"
        self.start = datetime.strptime(self._start, start_format)
        self.end = datetime.strptime(self._end, end_format)

        self.kwargs = kwargs

    def __repr__(self) -> str:
        args = f"datastream={self.datastream}, start={self._start}, end={self._end}"
        kwargs = ", ".join([f"{k}={v}" for k, v in self.kwargs.items()])
        return f"StorageRetrieverInput({args}, {kwargs})"
