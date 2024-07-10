import re
from typing import Callable

from .is_balanced import _is_balanced
from .template_registry import get_regex


class TemplateChunk:
    """Class to hold a chunk of a Template.

    A chunk is a smaller part of a template. E.g., the template "{a}{b}{c}d" has 4
    chunks: "{a}", "{b}", "{c}", and "d"."""

    def __init__(self, chunk: str) -> None:
        if not _is_balanced(chunk):
            raise ValueError(f"Unbalanced brackets in chunk: '{chunk}'")
        self.str = chunk
        self.var_name = self._get_variable_name(chunk)
        self.parts = self._get_parts(chunk)
        self.is_required = self._is_required(chunk)
        self.regex = self._generate_regex(chunk)

    def __repr__(self) -> str:
        return f"TemplateChunk({self.str})"

    @staticmethod
    def _get_variable_name(chunk: str) -> str | None:
        if "{" in chunk:
            start, stop = chunk.index("{"), chunk.index("}")
            return chunk[start + 1 : stop]
        return None

    @staticmethod
    def _get_parts(chunk: str) -> tuple[str, ...]:
        return tuple(re.split(r"[{}\[\]]", chunk))

    @staticmethod
    def _is_required(chunk: str) -> bool | None:
        if "{" in chunk:
            return "[" not in chunk
        return None

    @staticmethod
    def _generate_regex(chunk: str) -> str:
        regex_pattern = ""
        i = 0
        while i < len(chunk):
            char = chunk[i]
            if char == "{":
                var_start = i + 1
                var_end = chunk.index("}", var_start)
                var_name = chunk[var_start:var_end]
                regex_pattern += get_regex(var_name)
                i = var_end + 1
            elif char == "[":
                regex_pattern += "(?:"
                i += 1
            elif char == "]":
                regex_pattern += ")?"
                i += 1
            else:
                regex_pattern += re.escape(char)
                i += 1
        return regex_pattern

    def sub(
        self,
        value: str | (Callable[[], str]) | None,
        allow_missing: bool = False,
        fill: str | None = None,
    ) -> str:
        def remove_square_brackets(s: str) -> str:
            return s if self.is_required else s[1:-1]

        if callable(value):
            value = value()

        result = ""
        if self.var_name is None:
            result = self.str
        elif value is not None:
            result = self.str.replace(f"{{{self.var_name}}}", value)
            result = remove_square_brackets(result)
        elif allow_missing and fill:
            result = fill
        elif allow_missing:
            result = self.str
        elif self.is_required:
            raise ValueError(
                f"Could not make substitution for {self.var_name} in {self}"
            )
        return result
