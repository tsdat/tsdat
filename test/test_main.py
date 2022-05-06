import tempfile
from tsdat.main import app
from typer.testing import CliRunner

runner = CliRunner()


def test_schema_generation():
    tmp_dir = tempfile.TemporaryDirectory()
    result = runner.invoke(app, ["generate-schema", "--dir", tmp_dir.name])
    assert result.exit_code == 0

    schemas = ["retriever", "dataset", "quality", "storage", "pipeline"]

    for schema in schemas:
        assert f"Wrote {schema} schema file to {tmp_dir.name}" in result.stdout

    tmp_dir.cleanup()
