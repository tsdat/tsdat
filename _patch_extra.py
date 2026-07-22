import pathlib

files = [
    "tsdat/io/readers/csv_reader.py",
    "tsdat/io/readers/parquet_reader.py",
    "tsdat/io/readers/tar_reader.py",
    "tsdat/io/readers/zarr_reader.py",
    "tsdat/io/readers/zip_reader.py",
    "tsdat/io/retrievers/default_retriever.py",
    "tsdat/io/writers/csv_writer.py",
    "tsdat/io/writers/netcdf_writer.py",
    "tsdat/io/writers/parquet_writer.py",
    "tsdat/io/writers/zarr_writer.py",
]
for f in files:
    p = pathlib.Path(f)
    txt = p.read_text()
    orig = txt
    txt = txt.replace("from pydantic import BaseModel, Extra, Field", "from pydantic import BaseModel, ConfigDict, Field")
    txt = txt.replace("from pydantic import BaseModel, Extra", "from pydantic import BaseModel, ConfigDict")
    old = "class Parameters(BaseModel, extra=Extra.forbid):\n"
    new = "class Parameters(BaseModel):\n        model_config = ConfigDict(extra='forbid')\n"
    txt = txt.replace(old, new)
    if txt != orig:
        p.write_text(txt)
        print("Patched " + f)
    else:
        print("No change " + f)
