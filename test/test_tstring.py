from typing import Callable, Dict

import pytest

from tsdat.tstring import Template, TemplateChunk


@pytest.mark.parametrize(
    ("expected", "template", "value", "allow_missing"),
    (
        ("", "", None, True),
        ("", "", None, False),
        ("a", "a", "a", True),
        ("a", "a", lambda: "a", True),
        ("a", "{b}", "a", True),
        ("a", "[{b}]", "a", True),
        ("", "[-{b}]", None, False),
        ("[{b}]", "[{b}]", None, True),
    ),
)
def test_chunk_substitutions(
    expected: str,
    template: str,
    value: str | Callable[[], str] | None,
    allow_missing: bool,
):
    assert TemplateChunk(template).sub(value, allow_missing) == expected


@pytest.mark.parametrize(
    ("error", "template", "value", "allow_missing"),
    (
        (ValueError, "{a}", None, False),
        (ValueError, "{a", None, False),
        (ValueError, "[a}]", None, False),
    ),
)
def test_chunk_failures(
    error: Exception,
    template: str,
    value: str | Callable[[], str] | None,
    allow_missing: bool,
):
    with pytest.raises(error):
        TemplateChunk(template).sub(value, allow_missing=allow_missing)


@pytest.mark.parametrize(
    ("expected", "template", "mapping", "allow_missing"),
    (
        ("", "", dict(), True),
        ("a", "a", dict(), True),
        ("a", "{b}", dict(b="a"), True),
        ("a", "[{b}]", dict(b="a"), True),
        ("", "[-{b}]", None, False),
        ("[{b}]", "[{b}]", dict(), True),
        ("ab", "{a}[{b}]", dict(a="a", b="b"), True),
        ("a.b", "{a}[.{b}]", dict(a="a", b="b"), True),
        ("defg", "{a}{b}{c}g", dict(a="d", b="e", c="f"), True),
        ("defg", "{a}{b}{c}g", dict(a=lambda: "d", b=lambda: "e", c=lambda: "f"), True),
        ("d.e-gf", "{a}.{b}[-g{c}]", dict(a="d", b="e", c="f"), True),
        ("d.e-gf[-{d}]", "{a}.{b}[-g{c}][-{d}]", dict(a="d", b="e", c="f"), True),
    ),
)
def test_template_substitutions(
    expected: str, template: str, mapping: Dict[str, str], allow_missing: bool
):
    assert Template(template).substitute(mapping, allow_missing) == expected


@pytest.mark.parametrize(
    ("error", "template", "mapping", "allow_missing"),
    (
        (ValueError, "{a}", dict(), False),
        (ValueError, "{a}", None, False),
        (ValueError, "{a", dict(), False),
        (ValueError, "[a}]", dict(), False),
        (ValueError, "{a}{b}{c}", dict(), False),
    ),
)
def test_failures(
    error: Exception, template: str, mapping: Dict[str, str], allow_missing: bool
):
    with pytest.raises(error):
        Template(template).substitute(mapping, allow_missing=allow_missing)


def test_fill():
    template = Template("{a}.{b}[.{c}]")

    # Fill should fill in any values that are missing
    result = template.substitute(dict(a="x"), allow_missing=True, fill="*")
    assert result == "x.**"

    # Fill is only done if allow_missing is True
    with pytest.raises(ValueError):
        template.substitute(dict(a="x"), allow_missing=False, fill="y")


@pytest.mark.parametrize(
    ("expected", "mapping", "keywords"),
    (
        ("a.b.c", dict(a="a", b="b", c="c"), dict()),
        ("a.b.c", dict(), dict(a="a", b="b", c="c")),
        ("a.b.c", dict(a="a", b="b", c="d"), dict(c="c")),
        ("a.b", dict(a="d", b="e", c="f"), dict(a="a", b="b", c=None)),
    ),
)
def test_overrides(expected: str, mapping: Dict[str, str], keywords: Dict[str, str]):
    template = Template("{a}.{b}[.{c}]")
    assert template.substitute(mapping, **keywords) == expected


@pytest.mark.parametrize(
    ("expected", "template", "formatted"),
    (
        (dict(a="b"), "{a}", "b"),
        (dict(a="b"), "{a}.{a}", "b.b"),
        (dict(a="b"), "{a}{a}", "bb"),  # works because required is greedy
        (dict(a="bb"), "{a}[{a}]", "bb"),  # edge case. to fix use better separators
        (dict(a="b"), "{a}.[{a}]", "b.b"),  # e.g., with better separators
        (dict(a="a", b="b", c="c"), "{a}.{b}.{c}", "a.b.c"),
        (dict(a="a", b="b", c="c"), "{a}.{b}[.{c}]", "a.b.c"),
        (dict(a="a", b="b", c="c"), "{a}[.{b}].{c}", "a.b.c"),
        (dict(a="a", b="b", c="c"), "{a}.[{b}.]{c}", "a.b.c"),
        (dict(a="a", b=None, c="c"), "{a}.[{b}.]{c}", "a.c"),
        (dict(a="a", b="b", c="c"), "{a}[.de-{b}].{c}", "a.de-b.c"),
        (dict(a="a", b="b", c="c"), "[{a}.]{b}.{c}", "a.b.c"),
        (dict(a=None, b="b", c="c"), "[{a}.]{b}.{c}", "b.c"),
        (dict(a="a", b=None, c="c"), "{a}[.{b}].{c}", "a.c"),
        (
            dict(var1="a", opt1="foo", var2="b"),
            "{var1}[.{opt1}bar].{var2}",
            "a.foobar.b",
        ),
        # test with actual datastream components
        (
            dict(
                location_id="sgp",
                dataset_name="lidar",
                qualifier=None,
                temporal=None,
                data_level="a0",
            ),
            "{location_id}.{dataset_name}[-{qualifier}][-{temporal}].{data_level}",
            "sgp.lidar.a0",
        ),
        (
            dict(
                location_id="sgp",
                dataset_name="lidar",
                qualifier="z01",
                temporal="10m",
                data_level="a0",
            ),
            "{location_id}.{dataset_name}[-{qualifier}][-{temporal}].{data_level}",
            "sgp.lidar-z01-10m.a0",
        ),
        (
            dict(
                datastream="sgp.lidar-z01-10m.a0",
                location_id="sgp",
                dataset_name="lidar",
                qualifier="z01",
                temporal="10m",
                data_level="a0",
            ),
            "{datastream}",
            "sgp.lidar-z01-10m.a0",
        ),
        (
            dict(
                datastream="sgp.lidar-z01-10m.a0",
                location_id="sgp",
                dataset_name="lidar",
                qualifier="z01",
                temporal="10m",
                data_level="a0",
            ),
            "{datastream}--{datastream}",
            "sgp.lidar-z01-10m.a0--sgp.lidar-z01-10m.a0",
        ),
        (
            dict(
                location_id="sgp",
                dataset_name="lidar",
                qualifier="z01",
                temporal="10m",
                data_level="a0",
            ),
            "{location_id}.{dataset_name}[::{qualifier}:][::{temporal}:].{data_level}",
            "sgp.lidar::z01:::10m:.a0",  # weird format, just want an edge case
        ),
        (
            dict(
                location_id="sgp",
                dataset_name="lidar",
                z_id="z01",
                temporal=None,
                data_level="a0",
            ),
            "{location_id}.{dataset_name}.{z_id}[.{temporal}].{data_level}",
            "sgp.lidar.z01.a0",
        ),
    ),
)
def test_regex_extractions(expected: Dict[str, str], template: str, formatted: str):
    _template = Template(template)
    substitutions = _template.extract_substitutions(formatted)
    assert substitutions == expected


def test_manual_regex_extraction():
    # ARM datastream
    formatted = "mosthermocldphaseM1.c1"
    template = Template(
        "{location_id}{dataset_name}{facility}.{data_level}",
        r"^(?P<location_id>[a-z]{3})(?P<dataset_name>[a-zA-Z0-9]+)(?P<facility>[A-Z]{1}[0-9]+)\.(?P<data_level>[a-z0-9]{1}[0-9]{1})$",
    )
    expected = dict(
        location_id="mos", dataset_name="thermocldphase", facility="M1", data_level="c1"
    )

    assert template.extract_substitutions(formatted) == expected


def test_repr():
    template = Template("{a}{b}{c}")
    assert repr(template) == "Template('{a}{b}{c}')"


def test_str():
    template = Template("{a}{b}{c}")
    assert str(template) == "{a}{b}{c}"


@pytest.mark.parametrize(
    ("expected", "left", "right", "mapping"),
    (
        ("a.b/c", Template("{x}.{y}"), "{z}", dict(x="a", y="b", z="c")),
        ("a.b/z", Template("{x}.{y}"), "z", dict(x="a", y="b")),
        ("ab/c", Template("ab"), Template("{z}"), dict(z="c")),
        ("ab/z", Template("ab"), "z", dict()),
    ),
)
def test_div(
    expected: str, left: Template, right: Template | str, mapping: Dict[str, str]
):
    assert (left / right).substitute(mapping) == expected

    left /= right  # test idiv
    assert left.substitute(mapping) == expected


def test_div_error():
    with pytest.raises(ValueError):
        _ = Template("{a}") / "{"  # not balanced


@pytest.mark.parametrize(
    ("template", "variables"),
    (
        ("{a}.{b}.{c}", ("a", "b", "c")),
        ("{a}.{b}[{c}]", ("a", "b", "c")),
        ("{a}.{b}[.{c}]", ("a", "b", "c")),
        ("{a}1.{b}2[.{c}3]", ("a", "b", "c")),
    ),
)
def test_variables(template: str, variables: list[str]):
    assert Template(template).variables == variables
