# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
import os
import sys

project_dir = os.path.abspath("../../")
print(f"project_dir = {project_dir}")
sys.path.insert(0, project_dir)


# -- Project information -----------------------------------------------------

project = "tsdat"
copyright = "2022, Battelle Memorial Institute"
author = "tsdat"

# The full version, including alpha/beta/rc tags
# release = "0.4.1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.doctest",
    "sphinx.ext.ifconfig",
    "sphinx.ext.extlinks",
    "autoapi.extension",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_templates"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
html_css_files = [
    "css/custom.css",
]

# -- sphinx-napoleon configuration --------------------------------------------
# This is used to support google style docstrings.
napoleon_use_rtype = False
napoleon_use_param = True


# -- sphinx-autoapi configuration --------------------------------------------

autoapi_template_dir = "_templates/autoapi"

# Add directories for sphinx-autoapi to search in for code
autoapi_dirs = ["../../tsdat"]

autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
]

autoapi_python_class_content = "both"
autoapi_member_order = "groupwise"
autoapi_keep_files = False


# https://sphinx-autoapi.readthedocs.io/en/latest/reference/config.html#confval-autoapi_python_use_implicit_namespaces
autoapi_python_use_implicit_namespaces = True

# -- sphinx custom setup ------------------------------------------------------

from sphinx.domains.python import PythonDomain
from sphinx.application import Sphinx
from typing import Any, List
import re


class PatchedPythonDomain(PythonDomain):
    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):  # type: ignore
        if "refspecific" in node:
            del node["refspecific"]
        return super(PatchedPythonDomain, self).resolve_xref(
            env, fromdocname, builder, typ, target, node, contnode  # type: ignore
        )


def setup(app):  # type: ignore
    # https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html

    # Silence 'more than one target found for cross-reference' warning
    # https://github.com/sphinx-doc/sphinx/issues/3866
    app.add_domain(PatchedPythonDomain, override=True)  # type: ignore

    pattern = re.compile("\\-+")

    def process(
        app: Sphinx, what_: str, name: str, obj: Any, options: Any, lines: List[str]
    ) -> None:

        # Strip leading and trailing dashes from lines that only consist of dashes
        for idx, line in enumerate(lines):
            if pattern.match(line):
                lines[idx] = line.strip("-")

    app.connect("autodoc-process-docstring", process)  # type: ignore
    return app  # type: ignore
