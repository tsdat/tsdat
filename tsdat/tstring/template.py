from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Mapping

from .is_balanced import _is_balanced
from .template_chunk import TemplateChunk
from .template_registry import TEMPLATE_REGISTRY


class Template:
    """Python f-string implementation with lazy and optional variable substitutions.

    The template string is expected to be formatted in the same way as python f-strings,
    with variables that should be substituted wrapped in curly braces `{}`.
    Additionally, square brackets may be used around curly brackets and other text to
    mark that substitution as optional -- i.e. if the variable cannot be found then the
    text wrapped in the square brackets will be removed.


    Examples:

        `mapping = dict(a="x", b="y", c="z")`

        `TemplateString("{a}.{b}{c}w").substitute(mapping) # -> "x.yzw"`

        `TemplateString("{a}.{b}[.{c}]").substitute(mapping) # -> "x.y.z"`

        `TemplateString("{a}.{b}.{d}").substitute(mapping)  # raises ValueError`

        `TemplateString("{a}.{b}[.{d}]").substitute(mapping) # -> "x.y"`

        `TemplateString("{a}.{b}.{d}").substitute(mapping, True) # -> "x.y.{d}"`

    Args:
        template (str | Path): The template string. Variables to substitute should be
            wrapped by curly braces `{}`.
        regex (str | None, optional): A regex pattern used to extract the substitutions
            used to create a formatted string with this template. Generated
            automatically if not provided.
    """

    def __init__(self, template: str | Path, regex: str | None = None) -> None:
        _template = str(template)
        if not self._is_balanced(_template):
            raise ValueError(f"Unbalanced brackets in template string: '{template}'")
        self.template = _template
        self.chunks = self._get_chunks(_template)
        self.regex = regex or self._generate_regex(self.chunks)
        self.variables = tuple(
            chunk.var_name for chunk in self.chunks if chunk.var_name is not None
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.template!r})"

    def __str__(self) -> str:
        return self.template

    def __truediv__(self, other: Template | str) -> Template:
        if isinstance(other, str):
            other = Template(other)
        return Template(template=f"{self.template}/{other.template}")

    def __itruediv__(self, other: Template | str) -> Template:
        result = self / other
        self.template = result.template
        self.regex = result.regex
        self.chunks = result.chunks
        self.variables = result.variables
        return self

    @staticmethod
    def _get_chunks(template: str) -> tuple[TemplateChunk, ...]:
        pattern = re.compile(r"\{[^}]*\}|\[[^\]]*\]|[^[\]{}]+")  # splits by {} or [*{}]
        matches = pattern.findall(template)

        # We have to prevent regex patterns from having the same name
        seen: set[str] = set()

        chunks: list[TemplateChunk] = []
        for match in matches:
            chunk = TemplateChunk(match)
            if chunk.var_name in TEMPLATE_REGISTRY:
                # Allow substitutions like {datastream} while still matching things that
                # make up the datastream, e.g. {location_id}.{dataset_name}.{data_level}
                sub_regex = (
                    f"(?P<{chunk.var_name}>"
                    if chunk.var_name is not None and chunk.var_name not in seen
                    else "("
                )
                sub_template = TEMPLATE_REGISTRY[chunk.var_name]
                for sub_match in pattern.findall(sub_template):
                    sub_chunk = TemplateChunk(sub_match)
                    if sub_chunk.var_name is not None and sub_chunk.var_name in seen:
                        sub_chunk.regex = sub_chunk.regex.replace(
                            f"?P<{sub_chunk.var_name}>", ""
                        )
                    elif sub_chunk.var_name is not None:
                        seen.add(sub_chunk.var_name)
                    sub_regex += sub_chunk.regex
                chunk.regex = sub_regex + ")"
                seen.add(chunk.var_name)
            elif chunk.var_name is not None and chunk.var_name in seen:
                chunk.regex = chunk.regex.replace(f"?P<{chunk.var_name}>", "")
            elif chunk.var_name is not None:
                seen.add(chunk.var_name)
            chunks.append(chunk)
        return tuple(chunks)

    @staticmethod
    def _generate_regex(chunks: tuple[TemplateChunk, ...]) -> str:
        """Generates a regex pattern which can be used to extract the values substituted
        into a template string.

        Args:
            template (str): The template string to generate a regex pattern for.

        Returns:
            str: The regex pattern with named groups according to the template.
        """
        regex_pattern = "^"
        for chunk in chunks:
            regex_pattern += chunk.regex
        regex_pattern += "$"
        return regex_pattern

    @staticmethod
    def _is_balanced(template: str) -> bool:
        return _is_balanced(template)

    def substitute(
        self,
        mapping: Mapping[str, str | Callable[[], str] | None] | None = None,
        allow_missing: bool = False,
        fill: str | None = None,
        **kwds: str | Callable[[], str] | None,
    ) -> str:
        """Substitutes variables in a template string.

        Args:
            mapping (Mapping[str, str | Callable[[], str] | None] | None): A key-value pair
                of variable name to the value to replace it with. If the value is a
                string it is dropped-in directly. If it is a no-argument callable the
                return value of the callable is used. If it is None, then it is treated
                as missing and the action taken depends on the `allow_missing` parameter.
            allow_missing (bool, optional): Allow variables outside of square brackets to be
                missing, in which case they are left as-is, including the curly brackets.
                This is intended to allow users to perform some variable substitutions
                before all variables in the mapping are known. Defaults to False.
            fill (str, optional): Value to use to fill in missing substitutions
                (e.g., "*"). Only applied if 'allow_missing=True' (values in square
                brackets []). If None (the default) then no values will be filled.
            **kwds (str | Callable[[], str] | None): Optional extras to be merged into the
                mapping dict. If a keyword passed here has the same name as a key in the
                mapping dict, the value here would be used instead.

        Raises:
            ValueError: If any required substitutions cannot be made and fill is None.

        Returns:
            str: The template string with the appropriate substitutions made.
        """
        if mapping is None:
            mapping = {}
        mapping = {**mapping, **kwds}
        mapping = {k: v for k, v in mapping.items() if v is not None}

        results: list[str] = []
        for chunk in self.chunks:
            results.append(
                chunk.sub(
                    value=mapping.get(chunk.var_name)
                    if chunk.var_name is not None
                    else None,
                    allow_missing=allow_missing,
                    fill=fill,
                )
            )
        return "".join(results)

    def extract_substitutions(self, formatted_str: str) -> dict[str, str] | None:
        """Extracts the substitutions used to create the provided formatted string.

        Note that this is not guaranteed to return accurate results if the template
        is constructed such that separators between variables are ambiguous.

        Args:
            formatted_str (str): The formatted string

        Returns:
            dict[str, str]: A dictionary mapping each matched template variable to its
                value in the formatted string. Returns None if there are no matches.
        """
        match = re.match(self.regex, formatted_str)
        if match:
            return match.groupdict()
        else:
            return None
