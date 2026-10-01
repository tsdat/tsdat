"""CLI dispatch and command-line validation tests."""

import logging
from pathlib import Path
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from tsdat.cli import app
from tsdat.cli.registry import PipelineRegistry
from tsdat.pipeline.pipelines.transformation_pipeline import TransformationPipeline

runner = CliRunner()


def registry(tmp_path: Path, monkeypatch, triggers: dict[str, list[str]]):
    monkeypatch.chdir(tmp_path)
    for name, patterns in triggers.items():
        config = tmp_path / "pipelines" / name / "config" / "pipeline.yaml"
        config.parent.mkdir(parents=True)
        config.write_text("triggers:\n" + "".join(f"  - '{p}'\n" for p in patterns))
    pipeline = Mock()
    pipeline.__repr_name__ = Mock(return_value="ExamplePipeline")
    config_class = Mock()
    config_class.from_yaml.return_value.instantiate_pipeline.return_value = pipeline
    monkeypatch.setattr("tsdat.cli.registry.PipelineConfig", config_class)
    return PipelineRegistry(), pipeline, config_class


def test_default_runs_each_matching_input(tmp_path, monkeypatch):
    dispatch, pipeline, config = registry(
        tmp_path, monkeypatch, {"one": [".*one.*"], "two": [".*two.*"]}
    )
    assert dispatch.dispatch(["one-a", "one-b", "two-a", "unmatched"]) == (3, 0, 1)
    assert [call.args[0] for call in pipeline.run.call_args_list] == [
        ["one-a"],
        ["one-b"],
        ["two-a"],
    ]
    assert config.from_yaml.call_count == 3


def test_clump_runs_each_matching_pipeline_once(tmp_path, monkeypatch):
    dispatch, pipeline, config = registry(
        tmp_path, monkeypatch, {"one": [".*one.*"], "two": [".*two.*"]}
    )
    assert dispatch.dispatch(["one-a", "two-a", "one-b", "two-b"], clump=True) == (
        2,
        0,
        0,
    )
    assert [call.args[0] for call in pipeline.run.call_args_list] == [
        ["one-a", "one-b"],
        ["two-a", "two-b"],
    ]
    assert config.from_yaml.call_count == 2


def test_ambiguous_inputs_do_not_run_without_multidispatch(tmp_path, monkeypatch):
    dispatch, pipeline, _ = registry(
        tmp_path, monkeypatch, {"one": [".*shared.*"], "two": [".*shared.*"]}
    )
    with pytest.raises(ValueError, match="More than one pipeline"):
        dispatch.dispatch(["shared-a"], clump=True)
    pipeline.run.assert_not_called()
    assert dispatch.dispatch(
        ["shared-a", "shared-b"], clump=True, multidispatch=True
    ) == (2, 0, 0)
    assert [call.args[0] for call in pipeline.run.call_args_list] == [
        ["shared-a", "shared-b"],
        ["shared-a", "shared-b"],
    ]


def test_processing_failure_continues_and_reports(tmp_path, monkeypatch, caplog):
    dispatch, pipeline, _ = registry(tmp_path, monkeypatch, {"one": [".*one.*"]})
    pipeline.run.side_effect = [RuntimeError("broken"), None]
    with caplog.at_level(logging.INFO):
        assert dispatch.dispatch(["one-a", "one-b"]) == (1, 1, 0)
    assert (
        "Processing completed with 1 successes, 1 failures, and 0 skipped"
        in caplog.text
    )


def test_ingest_exit_status_and_missing_project(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    filepath = tmp_path / "input.csv"
    filepath.write_text("input")
    result = runner.invoke(app, ["ingest", str(filepath)])
    assert result.exit_code != 0
    assert "pipeline repository" in result.output

    registry(tmp_path, monkeypatch, {"one": [".*input.*"]})
    pipeline = Mock()
    pipeline.__repr_name__ = Mock(return_value="ExamplePipeline")
    pipeline.run.side_effect = RuntimeError("broken")
    monkeypatch.setattr(
        "tsdat.cli.registry.PipelineConfig.from_yaml",
        lambda _: Mock(instantiate_pipeline=lambda: pipeline),
    )
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["ingest", str(filepath)])
    assert result.exit_code == 1
    assert "1 failures" in caplog.text


def test_vap_rejects_invalid_dates(tmp_path):
    config = tmp_path / "pipeline.yaml"
    config.write_text("config")
    for invalid in ("20231301", "20230230", "20230101.250000", "20230101123456"):
        result = runner.invoke(
            app, ["vap", str(config), "-b", invalid, "-e", "20230102"]
        )
        assert result.exit_code != 0
        assert "valid date" in result.output


def test_vap_rejects_ingest_pipeline(tmp_path, monkeypatch):
    config = tmp_path / "pipeline.yaml"
    config.write_text("config")
    pipeline = Mock()
    pipeline.__repr_name__ = Mock(return_value="IngestPipeline")
    monkeypatch.setattr(
        "tsdat.cli.run.PipelineConfig.from_yaml",
        lambda _: Mock(instantiate_pipeline=lambda: pipeline),
    )
    result = runner.invoke(
        app, ["vap", str(config), "-b", "20230101", "-e", "20230102"]
    )
    assert result.exit_code != 0
    assert "TransformationPipeline" in result.output


def test_vap_success_and_failure(tmp_path, monkeypatch, caplog):
    config = tmp_path / "pipeline.yaml"
    config.write_text("config")
    pipeline = Mock(spec=TransformationPipeline)
    pipeline.__repr_name__ = Mock(return_value="VAP")
    monkeypatch.setattr(
        "tsdat.cli.run.PipelineConfig.from_yaml",
        lambda _: Mock(instantiate_pipeline=lambda: pipeline),
    )
    args = ["vap", str(config), "-b", "20230101", "-e", "20230102.123456"]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    pipeline.run.assert_called_once_with(inputs=["20230101.000000", "20230102.123456"])
    pipeline.run.side_effect = RuntimeError("broken")
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, args)
    assert result.exit_code == 1
    assert "1 failures" in caplog.text
