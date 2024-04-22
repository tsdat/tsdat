from pathlib import Path
from typing import (
    Any,
    Dict,
)

import yaml
from jsonpointer import set_pointer  # type: ignore


def read_yaml(filepath: Path) -> Dict[Any, Any]:
    return list(yaml.safe_load_all(filepath.read_text(encoding="UTF-8")))[0]
