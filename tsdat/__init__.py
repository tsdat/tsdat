# IDEA: Add a typer CLI to do a guided setup of a basic filesystem ingest

# IDEA: Add a __main__.py file parallel with this one which provides a number of typer
# commands that can be run. Users would invoke it via python -m tsdat, or we could use
# entrypoints / console scripts in setup.py to export the typer app as it's own thing.

# IDEA: Plugin architecture where we can add high-quality standardization pipelines to
# our package so users can simply run `tsdat standardize path/to/data.csv` and it will
# do it's thing with the appropriate plugin that matches the input key
