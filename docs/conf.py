# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
import locale

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "digital_land"
copyright = "2024, Team"
author = "Team"
release = "1.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ["_templates"]
exclude_patterns = []

language = "python"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]

sys.path.insert(0, os.path.abspath("../digital_land"))

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # For Google or NumPy style docstrings
]
html_theme = "sphinx_rtd_theme"

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
