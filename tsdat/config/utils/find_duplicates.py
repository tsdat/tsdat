from typing import (
    List,
    Sequence,
    Set,
)

from jsonpointer import set_pointer  # type: ignore

from ._named_class import _NamedClass


def find_duplicates(entries: Sequence[_NamedClass]) -> List[str]:
    duplicates: List[str] = []
    seen: Set[str] = set()
    for entry in entries:
        if entry.name in seen:
            duplicates.append(entry.name)
        else:
            seen.add(entry.name)
    return duplicates
