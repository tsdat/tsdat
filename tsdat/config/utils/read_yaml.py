import yaml
from jsonpointer import set_pointer  # type: ignore
from pathlib import Path
from typing import Any, Dict


def read_yaml(filepath: Path) -> Dict[Any, Any]:
    return list(yaml.safe_load_all(filepath.read_text(encoding="UTF-8")))[0]
