#!/usr/bin/env python3
import typer
from tsdat.utils import generate_schema

app = typer.Typer(add_completion=False)


app.command()(generate_schema)


@app.callback()
def callback():
    pass


if __name__ == "__main__":
    app()
