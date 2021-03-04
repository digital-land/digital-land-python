# Digital Land Pipeline

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/digital-land.svg)](https://pypi.python.org/pypi/digital-land/)
[![Build](https://github.com/digital-land/pipeline/workflows/Continuous%20Integration/badge.svg)](https://github.com/digital-land/pipeline/actions?query=workflow%3A%22Continuous+Integration%22)
[![Coverage](https://coveralls.io/repos/github/digital-land/digital-land-python/badge.svg?branch=main)](https://coveralls.io/github/digital-land/digital-land-python?branch=main)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/digital-land/digital-land-python/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://black.readthedocs.io/en/stable/)


*Python command-line tools for collecting and converting resources into a dataset*

## Installation

    pip3 install digital-land

## Command line

    $ digital-land --help

    Usage: digital-land [OPTIONS] COMMAND [ARGS]...

    Commands:
      collect     collect CSV and other resources into a digital-land collection
      convert     convert a file from XLS to UTF-8 encoded CSV
      normalise:  strip whitespace and null fields, remove blank rows and columns
      map:        map misspelt field names to those in a schema
      harmonise:  harmonise field values to the schema types
      transform:  map field names to another data model

    Options:
      --version  Show the version and exit.
      --help     Show this message and exit.

## Development environment

The GDAL tools are required to convert geographic data, and in order for all of the tests to pass.

Development requires Python 3.6.2 or later, we recommend using a [virtual environment](https://docs.python.org/3/library/venv.html):

    make init
    make
    python -m digital-land --help

## Release procedure

Update the tagged version number:

    make bump

Build the wheel and egg files:

    make dist

Push to GitHub:

    git push && git push --tags

Wait for the [continuous integration tests](https://pypi.python.org/pypi/digital-land/) to pass and then upload to [PyPI](https://pypi.python.org/pypi/digital-land/):

    make upload

# Licence

The software in this project is open source and covered by the [LICENSE](LICENSE) file.

Individual datasets copied into this repository may have specific copyright and licensing, otherwise all content and data in this repository is [© Crown copyright](http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/copyright-and-re-use/crown-copyright/) and available under the terms of the [Open Government 3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
