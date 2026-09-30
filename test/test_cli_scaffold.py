"""Integration tests for the pipeline repository and pipeline scaffold commands."""

import os
import subprocess
import sys
from pathlib import Path

from typer.testing import CliRunner

from tsdat.cli import app

runner = CliRunner()


def init_project(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "project"])
    assert result.exit_code == 0, result.output
    return tmp_path / "project"


def test_init_prompts_for_destination(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init"], input="prompted-project\n")
    assert result.exit_code == 0, result.output
    assert "Where should the new pipeline repository be created?" in result.output
    assert (tmp_path / "prompted-project" / "runner.py").is_file()


def test_init_copies_project_and_refuses_existing_destination(tmp_path, monkeypatch):
    project = init_project(tmp_path, monkeypatch)
    for name in (
        "runner.py",
        "pipelines/example_pipeline/test/data/expected/morro.buoy_z06-waves.a1.20201201.000000.nc",
        "templates/ingest/cookiecutter.json",
        "templates/vap/cookiecutter.json",
        ".vscode/.env",
        ".github/workflows/tests.yml",
        ".gitignore",
    ):
        assert (project / name).is_file(), name
    marker = project / "marker"
    marker.write_text("preserve")
    result = runner.invoke(app, ["init", "project"])
    assert result.exit_code != 0
    assert marker.read_text() == "preserve"


def test_create_pipeline_requires_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        [
            "create-pipeline",
            "ingest",
            "--ingest-title",
            "Sample",
            "--ingest-location",
            "PNNL",
            "--ingest-description",
            "Example",
            "--data-standards",
            "basic",
            "--no-use-custom-data-reader",
            "--no-use-custom-data-converter",
            "--no-use-custom-qc",
        ],
    )
    assert result.exit_code != 0
    assert "pipeline repository" in result.output


def test_create_ingest_and_vap_and_refuse_duplicate(tmp_path, monkeypatch):
    project = init_project(tmp_path, monkeypatch)
    monkeypatch.chdir(project)
    result = runner.invoke(
        app,
        [
            "create-pipeline",
            "ingest",
            "--ingest-title",
            "Sample",
            "--ingest-location",
            "PNNL",
            "--ingest-description",
            "Example",
            "--data-standards",
            "basic",
            "--no-use-custom-data-reader",
            "--no-use-custom-data-converter",
            "--no-use-custom-qc",
        ],
        input="y\ny\ny\n",
    )
    assert result.exit_code == 0, result.output
    assert (project / "pipelines/sample/config/dataset.yaml").is_file()
    assert not (project / "pipelines/sample/readers.py").exists()
    duplicate = runner.invoke(
        app,
        [
            "create-pipeline",
            "ingest",
            "--ingest-title",
            "Sample",
            "--ingest-location",
            "PNNL",
            "--ingest-description",
            "Example",
            "--data-standards",
            "basic",
            "--no-use-custom-data-reader",
            "--no-use-custom-data-converter",
            "--no-use-custom-qc",
        ],
        input="\n",
    )
    assert duplicate.exit_code != 0
    assert "already exists" in duplicate.output
    result = runner.invoke(
        app,
        [
            "create-pipeline",
            "vap",
            "--title",
            "Sample",
            "--location",
            "PNNL",
            "--description",
            "Example",
            "--no-use-custom-data-converter",
            "--no-use-custom-qc",
        ],
        input="y\ny\ny\n",
    )
    assert result.exit_code == 0, result.output
    assert (project / "pipelines/vap_sample/config/dataset.yaml").is_file()
    assert not (project / "pipelines/vap_sample/qc.py").exists()


def test_cli_works_outside_source_tree(tmp_path):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-m", "tsdat", "init", "example"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "example" / "runner.py").is_file()
