#!/usr/bin/env python

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

for path in sorted(Path("tsdat").glob("**/*.py")):
    # Exclude __init__.py, __main__.py, _version.py
    if path.name.startswith("_"):
        continue

    # TODO: Not sure of the reference need for this file? But refactor changed this file up a bit,
    #  so some examination is warranted to see what this might break.
    # base.py doesn't need API docs, main.py is too small to be useful
    if path.name in ["main.py"]:
        continue

    module_path = path.with_suffix("")
    doc_path = path.relative_to("tsdat").with_suffix(".md")
    full_doc_path = Path("API", doc_path)

    nav[module_path.parts] = doc_path

    with mkdocs_gen_files.open(full_doc_path, "w") as f:
        ident = ".".join(module_path.parts)
        print("::: " + ident, file=f)

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("API/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
