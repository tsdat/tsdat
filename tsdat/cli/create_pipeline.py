from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import typer

app = typer.Typer(
    help="Create an ingest or VAP pipeline in the current pipeline repository."
)


def _repository() -> Path:
    root = Path.cwd()
    required = [root / "pipelines" / "__init__.py"] + [
        root / "templates" / kind / "cookiecutter.json" for kind in ("ingest", "vap")
    ]
    if not all(path.is_file() for path in required):
        raise typer.BadParameter(
            "Run this command from the root of a pipeline repository created with "
            "'tsdat init' (requires pipelines/ and templates/ingest and templates/vap)."
        )
    return root


def _generator_dependencies():
    from cookiecutter.main import cookiecutter
    from slugify import slugify

    return cookiecutter, slugify


def _choose_module(default: str) -> str:
    if not valid_module(default):
        return validated_prompt(
            "What module name would you like to use?\n", valid_module
        )
    if not typer.confirm(
        f"'{default}' will be the module name (the folder created under 'pipelines/') Is this OK?\n",
        default=True,
    ):
        return validated_prompt(
            "What would you like to rename the module to?\n", valid_module
        )
    return default


def _generate(
    kind: str, context: dict[str, Any], cookiecutter: Callable[..., Any]
) -> None:
    root = _repository()
    module = context["module"]
    if not valid_module(module):
        raise typer.BadParameter("Choose a different pipeline module name.")
    cookiecutter(
        str(root / "templates" / kind),
        no_input=True,
        output_dir=str(root / "pipelines"),
        extra_context=context,
    )
    typer.echo(f"Created new {kind} pipeline in {root / 'pipelines' / module}")


def _to_cookiecutter_bool(value: bool) -> Literal["yes", "no"]:
    return "yes" if value else "no"


def validated_prompt(text: str, validator: Callable[[Any], bool], **kwargs: Any) -> Any:
    value = typer.prompt(text, **kwargs)
    while not validator(value):
        value = typer.prompt(text, **kwargs)
    return value


def valid_module(module: str) -> bool:
    matches_regex = bool(re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", module))
    if not matches_regex:
        typer.echo(
            f"'{module}' is not a valid python module name. Module names must only"
            " contain alphanumeric and '_' characters and should start with a letter."
            " Using lowercase with words separated by '_' is considered best practice."
        )
    already_exists = (Path("pipelines") / module).exists()
    if already_exists:
        typer.echo(
            f"'{module}' already exists in the pipelines/ folder. This tool can only be"
            " used to add new pipelines, not to modify or extend an existing pipeline."
            " Please enter a new module name or abort this command (ctrl-C)."
        )
    return matches_regex and not already_exists


def valid_classname(classname: str) -> bool:
    is_valid = bool(re.match(r"^[A-Z][a-zA-Z0-9_]+$", classname))
    if not is_valid:
        typer.echo(
            f"'{classname}' is not a valid python class name. Class names must only"
            " contain alphanumeric and '_' characters and should start with a capital"
            " letter. Using CamelCase is considered best practice."
        )
    return is_valid


@app.command()
def ingest(
    ingest_title: str = typer.Option(
        ...,
        help="The title of the ingest excluding any location details. This will be used"
        " to generate a default pipeline class name and the folder in which it lives."
        " E.g., for a title of 'Halo Lidar', 'HaloLidar' would be the default name of"
        " the pipeline and 'pipelines/halo_lidar' would be the folder created to"
        " contain its code. Note that a later prompt will appear to confirm or modify"
        " these defaults.",
        prompt="What title do you want to give this ingest?\n",
    ),
    ingest_location: str = typer.Option(
        ...,
        help="A label for the location where the data are collected.",
        prompt="What label should be used for the location of the ingest? (E.g., PNNL,"
        " San Francisco, etc.)\n",
    ),
    ingest_description: str = typer.Option(
        ...,
        help="A brief description of the ingest that will be used in the ingest's"
        " README file and in the metadata produced by the pipeline.",
        prompt="Briefly describe the ingest\n",
    ),
    data_standards: str = typer.Option(
        ...,
        help="Data standardization conventions to hold the dataset to. Choose from "
        "'basic'(standard CF conventions), 'ACDD' (Attribute Conventions for Data "
        "Discovery), or 'IOOS' (Integrated Ocean Observing System) standards.",
        prompt="Data standards to use with the ingest dataset ['basic','ACDD','IOOS']\n",
    ),
    use_custom_data_reader: bool = typer.Option(
        False,
        help="Flag to generate boilerplate code for handling inputs stored in a data"
        " format not supported by tsdat out-of-the-box. To see a complete list of"
        " built-in data readers visit"
        " https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/io/readers",
        prompt="Do you want to use a custom DataReader?\n",
    ),
    use_custom_data_converter: bool = typer.Option(
        False,
        help="Flag to generate boilerplate code to apply custom convertions or other"
        " preprocessing actions to the raw data before it enters the data pipeline. Use"
        " this if the built-in data converters are not sufficient. To see a complete"
        " list of built-in DataConverters visit"
        " https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/io/converters",
        prompt="Do you want to use a custom DataConverter?\n",
    ),
    use_custom_qc: bool = typer.Option(
        False,
        help="Flag to generate boilerplate code for applying custom quality checks or"
        " corrections to the data. Use this if the built-in QualityCheckers or"
        " QualityHandlers are not sufficient. To see a complete list of "
        " list of built-in QualityCheckers and QualityHandlers visit"
        " https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/qc",
        prompt="Do you want to use a custom QualityChecker or QualityHandler?\n",
    ),
):
    """Ingest cookiecutter wrapper that adds validation and better dynamic defaults."""

    _repository()
    cookiecutter, slugify = _generator_dependencies()
    module = _choose_module(slugify(ingest_title, separator="_"))
    classname = module.replace("_", " ").title().replace(" ", "")
    location_id = slugify(ingest_location, separator="_")

    if not typer.confirm(
        f"'{classname}' will be the name of your IngestPipeline class (the python class"
        " containing your custom python code hooks). Is this OK?\n",
        default=True,
    ):
        classname = validated_prompt(
            "What would you like to rename the pipeline class to?\n", valid_classname
        )

    if not typer.confirm(
        f"'{location_id}' will be the short label used to represent the location where"
        " the data are collected. Is this OK?\n",
        default=True,
    ):
        location_id = typer.prompt(
            "What would you like to rename the location identifier to?\n"
        )

    cookiedough: dict[str, Any] = {
        "ingest_name": ingest_title,
        "ingest_location": ingest_location,
        "ingest_description": ingest_description,
        "data_standards": data_standards,
        "use_custom_data_reader": _to_cookiecutter_bool(use_custom_data_reader),
        "use_custom_data_converter": _to_cookiecutter_bool(use_custom_data_converter),
        "use_custom_qc": _to_cookiecutter_bool(use_custom_qc),
        "module": module,
        "classname": classname,
        "location_id": location_id,
    }

    typer.echo(
        "\nGenerating templates/ingest cookiecutter with the following answers:\n"
        f" {cookiedough}\n"
    )

    _generate("ingest", cookiedough, cookiecutter)


@app.command()
def vap(
    title: str = typer.Option(
        ...,
        help="The title of the pipeline excluding any location details. This will be used"
        " to generate a default pipeline class name and the folder in which it lives."
        " E.g., for a title of 'Halo Lidar', 'HaloLidar' would be the default name of"
        " the pipeline and 'pipelines/vap_halo_lidar' would be the folder created to"
        " contain its code. Note that a later prompt will appear to confirm or modify"
        " these defaults.",
        prompt="What title do you want to give this pipeline?\n",
    ),
    location: str = typer.Option(
        ...,
        help="A label for the location where the pipeline should run.",
        prompt="What label should be used for the location of the pipeline? (E.g., PNNL,"
        " San Francisco, etc.)\n",
    ),
    description: str = typer.Option(
        ...,
        help="A brief description of the pipeline that will be used in the pipeline's"
        " README file and in the metadata produced by the pipeline.",
        prompt="Briefly describe the pipeline\n",
    ),
    use_custom_data_converter: bool = typer.Option(
        False,
        help="Flag to generate boilerplate code to apply custom conversions or other"
        " preprocessing actions to the data before it enters the data pipeline. Use"
        " this if the built-in data converters are not sufficient. To see a complete"
        " list of built-in DataConverters visit"
        " https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/io/converters",
        prompt="Do you want to use a custom DataConverter?\n",
    ),
    use_custom_qc: bool = typer.Option(
        False,
        help="Flag to generate boilerplate code for applying custom quality checks or"
        " corrections to the data. Use this if the built-in QualityCheckers or"
        " QualityHandlers are not sufficient. To see a complete list of "
        " list of built-in QualityCheckers and QualityHandlers visit"
        " https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/qc",
        prompt="Do you want to use a custom QualityChecker or QualityHandler?\n",
    ),
):
    """Pipeline cookiecutter wrapper that adds validation and better dynamic defaults."""

    _repository()
    cookiecutter, slugify = _generator_dependencies()
    module = _choose_module("vap_" + slugify(title, separator="_"))
    classname = module.replace("_", " ").title().replace(" ", "")
    location_id = slugify(location, separator="_")

    if not typer.confirm(
        f"'{classname}' will be the name of your TransformationPipeline class (the python class"
        " containing your custom python code hooks). Is this OK?\n",
        default=True,
    ):
        classname = validated_prompt(
            "What would you like to rename the pipeline class to?\n", valid_classname
        )

    if not typer.confirm(
        f"'{location_id}' will be the short label used to represent the location where"
        " the data are collected. Is this OK?\n",
        default=True,
    ):
        location_id = typer.prompt(
            "What would you like to rename the location identifier to?\n"
        )

    cookiedough: dict[str, Any] = {
        "vap_name": title,
        "location": location,
        "vap_description": description,
        "use_custom_data_converter": _to_cookiecutter_bool(use_custom_data_converter),
        "use_custom_qc": _to_cookiecutter_bool(use_custom_qc),
        "module": module,
        "classname": classname,
        "location_id": location_id,
    }

    typer.echo(
        "\nGenerating templates/vap cookiecutter with the following answers:\n"
        f" {cookiedough}\n"
    )

    _generate("vap", cookiedough, cookiecutter)
