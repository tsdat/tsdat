"""Compatibility entry point for creating a pipeline in this repository.

The same generator is exposed as ``tsdat create-pipeline``.
"""

from tsdat.cli.create_pipeline import app


if __name__ == "__main__":
    app()
