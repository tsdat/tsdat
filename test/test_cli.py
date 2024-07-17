import tempfile

from typer.testing import CliRunner

from tsdat.cli import app

runner = CliRunner()


def test_schema_generation():
    # ioos
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(app, ["generate-schema", "--dir", tmp_dir])
        assert result.exit_code == 0
        assert "tsdat dataset standards" in result.stdout

    # acdd
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            app, ["generate-schema", "--dir", tmp_dir, "--standards", "acdd"]
        )
        assert result.exit_code == 0
        assert "acdd dataset standards" in result.stdout

    # ioos
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            app, ["generate-schema", "--dir", tmp_dir, "--standards", "ioos"]
        )
        assert result.exit_code == 0
        assert "ioos dataset standards" in result.stdout
