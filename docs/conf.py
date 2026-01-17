# Configuration file for the Sphinx documentation builder.
import os
import sys
from datetime import date

# Add src to path so autodoc can find the package
sys.path.insert(0, os.path.abspath("../src"))

import stabilize

# -- Project information -----------------------------------------------------
project = "Stabilize"
copyright = f"{date.today().year}, Stabilize Contributors"
author = "Stabilize Team"
release = stabilize.__version__
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",  # Google/NumPy style docstrings
    "sphinx.ext.todo",
    "sphinx_rtd_theme",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_theme_options = {
    "navigation_depth": 4,
    "collapse_navigation": False,
    "sticky_navigation": True,
}

# -- Extension configuration -------------------------------------------------
todo_include_todos = True
autodoc_member_order = "bysource"
autodoc_typehints = "description"
