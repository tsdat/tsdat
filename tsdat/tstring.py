from __future__ import annotations

import re
from typing import Callable, Mapping, Match

__all__ = ("Template",)

_SQUARE_BRACKET_REGEX = r"\[(.*?)\]"
_CURLY_BRACKET_REGEX = r"\{(.*?)\}"


def _substitute(
    template: str,
    mapping: Mapping[str, str | Callable[[], str] | None] | None = None,
    allow_missing: bool = False,
    **kwds: str | Callable[[], str] | None,
) -> str:
    """Substitutes variables in a template string.

    The template string is expected to be formatted in the same way as python f-strings,
    with variables that should be substituted wrapped in curly braces `{}`.
    Additionally, square brackets may be used around curly brackets and other text to
    mark that substitution as optional -- i.e. if the variable cannot be found then the
    text wrapped in the square brackets will be removed.

    Examples:

        `mapping = dict(a="x", b="y", c="z")`

        `substitute("{a}.{b}{c}w", mapping) == "x.yzw"  # True`

        `substitute("{a}.{b}[.{c}]", mapping) == "x.y.z"  # True`

        `substitute("{a}.{b}[.{d}]", mapping) == "x.y"  # True`

        `substitute("{a}.{b}.{d}", mapping, True) == "x.y.{d}"  # True`

        `substitute("{a}.{b}.{d}", mapping, False)  # raises ValueError

    Args:
        template (str): The template string. Variables to substitute should be wrapped
            by curly braces `{}`.
        mapping (Mapping[str, str | Callable[[], str] | None] | None): A key-value pair
            of variable name to the value to replace it with. If the value is a
            string it is dropped-in directly. If it is a no-argument callable the
            return value of the callable is used. If it is None, then it is treated
            as missing and the action taken depends on the `allow_missing` parameter.
        allow_missing (bool, optional): Allow variables outside of square brackets to be
            missing, in which case they are left as-is, including the curly brackets.
            This is intended to allow users to perform some variable substitutions
            before all variables in the mapping are known. Defaults to False.
        **kwds (str | Callable[[], str] | None): Optional extras to be merged into the
            mapping dict. If a keyword passed here has the same name as a key in the
            mapping dict, the value here would be used instead.

    Raises:
        ValueError: If the substitutions cannot be made due to missing variables.

    Returns:
        str: The template string with the appropriate substitutions made.
    """
    if mapping is None:
        mapping = {}
    mapping = {**mapping, **kwds}

    def _sub_curly(match: Match[str]) -> str:
        # group(1) returns string without {}, group(0) returns with {}
        # result is we only do replacements that we can actually do.
        res = mapping.get(match.group(1))
        if callable(res):
            res = res()
        if allow_missing and res is None:
            res = match.group(0)
        elif res is None:
            raise ValueError(f"Substitution cannot be made for key '{match.group(1)}'")
        return res

    def _sub_square(match: Match[str]) -> str:
        # make curly substitutions inside of square brackets or remove the whole thing
        # if substitutions cannot be made.
        try:
            resolved = re.sub(_CURLY_BRACKET_REGEX, _sub_curly, match.group(1))
            return resolved if resolved != match.group(1) else ""
        except ValueError:
            return ""

    squared = re.sub(_SQUARE_BRACKET_REGEX, _sub_square, template)
    resolved = re.sub(_CURLY_BRACKET_REGEX, _sub_curly, squared)

    return resolved


def _generate_regex(template: str) -> str:
    """Generates a regex pattern which can be used to extract the values substituted
    into a template string.

    Args:
        template (str): The template string to generate a regex pattern for.

    Returns:
        str: The regex pattern with named groups according to the template.
    """
    regex_pattern = "^"

    i = 0
    while i < len(template):
        char = template[i]
        if char == "{":
            var_start = i + 1
            var_end = template.index("}", var_start)
            var_name = template[var_start:var_end]
            regex_pattern += f"(?P<{var_name}>[_a-zA-Z0-9]+)"
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

    regex_pattern += "$"
    return regex_pattern


# def _generate_regex(template: str) -> str:
#     regex_pattern = "^"
#     i = 0
#     while i < len(template):
#         char = template[i]
#         if char == "{":
#             var_start = i + 1
#             var_end = template.index("}", var_start)
#             var_name = template[var_start:var_end]
#             regex_pattern += f"(?P<{var_name}>[^.*]+)?"
#             i = var_end + 1
#         elif char == "[":
#             regex_pattern += "(?:"
#             i += 1
#         elif char == "]":
#             regex_pattern += ")?"
#             i += 1
#         else:
#             regex_pattern += re.escape(char)
#             i += 1
#     regex_pattern += "$"
#     return regex_pattern


# def _generate_regex(template: str) -> str:
#     regex_pattern = "^"
#     is_optional = False
#     optional_part = ""

#     i = 0
#     while i < len(template):
#         char = template[i]
#         if char == "{":
#             var_start = i + 1
#             var_end = template.index("}", var_start)
#             var_name = template[var_start:var_end]

#             if is_optional:
#                 # regex_pattern += f"(?:{re.escape(optional_part)}(?P<{var_name}>[^.]*))?"
#                 regex_pattern += f"(?:{re.escape(optional_part)}(?P<{var_name}>.*?))?"
#                 optional_part = ""
#             else:
#                 regex_pattern += f"(?P<{var_name}>[^.]*)"

#             i = var_end + 1
#         elif char == "[":
#             is_optional = True
#             optional_part = ""
#             i += 1
#         elif char == "]":
#             is_optional = False
#             regex_pattern += re.escape(optional_part) + ")?"
#             optional_part = ""
#             i += 1
#         else:
#             if is_optional:
#                 optional_part += char
#             else:
#                 regex_pattern += re.escape(char)
#             i += 1

#     regex_pattern += "$"
#     return regex_pattern


# def _generate_regex(template: str) -> str:
#     regex_pattern = "^"
#     is_optional = False
#     optional_part = ""

#     i = 0
#     while i < len(template):
#         char = template[i]
#         if char == "{":
#             var_start = i + 1
#             var_end = template.index("}", var_start)
#             var_name = template[var_start:var_end]

#             if is_optional:
#                 regex_pattern += f"(?:{re.escape(optional_part)}(?P<{var_name}>.*?))?"
#                 optional_part = ""
#             else:
#                 regex_pattern += f"(?P<{var_name}>.*?)"

#             i = var_end + 1
#         elif char == "[":
#             is_optional = True
#             optional_part = ""
#             i += 1
#         elif char == "]":
#             is_optional = False
#             if optional_part:
#                 regex_pattern += f"(?:{re.escape(optional_part)})?"
#             optional_part = ""
#             i += 1
#         else:
#             if is_optional:
#                 optional_part += char
#             else:
#                 regex_pattern += re.escape(char)
#             i += 1

#     regex_pattern += "$"
#     return regex_pattern


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
        template (str): The template string. Variables to substitute should be wrapped
            by curly braces `{}`.
        regex (str | None, optional): A regex pattern used to extract the substitutions
            used to create a formatted string with this template. Generated
            automatically if not provided.
    """

    def __init__(self, template: str, regex: str | None = None) -> None:
        """"""
        if not self._is_balanced(template):
            raise ValueError(f"Unbalanced brackets in template string: '{template}'")
        self.template = template
        self.regex = regex or _generate_regex(template)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.template!r})"

    def __str__(self) -> str:
        return self.template

    @classmethod
    def _is_balanced(cls, template: str):
        stack: list[str] = []
        for char in template:
            if char in "{[":
                stack.append("}" if char == "{" else "]")
            elif char in "}]":
                if not stack or char != stack.pop():
                    return False
        return len(stack) == 0

    def substitute(
        self,
        mapping: Mapping[str, str | Callable[[], str] | None] | None = None,
        allow_missing: bool = False,
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
            **kwds (str | Callable[[], str] | None): Optional extras to be merged into the
                mapping dict. If a keyword passed here has the same name as a key in the
                mapping dict, the value here would be used instead.

        Raises:
            ValueError: If the substitutions cannot be made due to missing variables.

        Returns:
            str: The template string with the appropriate substitutions made.
        """
        return _substitute(self.template, mapping, allow_missing, **kwds)

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
