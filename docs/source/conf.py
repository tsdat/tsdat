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
project_dir = os.path.abspath('../../')
print(f"project_dir = {project_dir}")
sys.path.insert(0, project_dir)


# -- Project information -----------------------------------------------------

project = 'tsdat'
copyright = '2021, Carina Lansing, Maxwell Levin'
author = 'Carina Lansing, Maxwell Levin'

# The full version, including alpha/beta/rc tags
release = '0.2.2'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx_rtd_theme"
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']


# -- sphinx-autoapi configuration --------------------------------------------

# Add sphinx-autoapi extension
extensions.append('autoapi.extension')

# Add directories for sphinx-autoapi to search in for code
autoapi_dirs = [
    '../../tsdat'
    ]

# https://sphinx-autoapi.readthedocs.io/en/latest/reference/config.html#confval-autoapi_python_use_implicit_namespaces
autoapi_python_use_implicit_namespaces = True

# Silence 'more than one target found for cross-reference' warning
# https://github.com/sphinx-doc/sphinx/issues/3866
from sphinx.domains.python import PythonDomain
class PatchedPythonDomain(PythonDomain):
    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):
        if 'refspecific' in node:
            del node['refspecific']
        return super(PatchedPythonDomain, self).resolve_xref(
            env, fromdocname, builder, typ, target, node, contnode)
def setup(sphinx):
    sphinx.add_domain(PatchedPythonDomain, override=True)

