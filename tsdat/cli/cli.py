#!/usr/bin/env python3

import typer

from .generate_schema.generate_schema import generate_schema

app = typer.Typer(no_args_is_help=True)


app.command(help="Generate schemas to validate yaml configuration files.")(
    generate_schema
)


@app.callback()
def callback():
    pass
