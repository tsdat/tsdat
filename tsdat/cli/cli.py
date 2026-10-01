#!/usr/bin/env python3

import typer

from .create_pipeline import app as create_pipeline_app
from .generate_schema.generate_schema import generate_schema
from .init import init
from .run import ingest, vap

app = typer.Typer(no_args_is_help=True)
app.command()(init)
app.command()(ingest)
app.command()(vap)
app.add_typer(create_pipeline_app, name="create-pipeline")


app.command(help="Generate schemas to validate yaml configuration files.")(
    generate_schema
)


@app.callback()
def callback():
    pass
