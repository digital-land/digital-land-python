# Digital Land Pipeline

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/digital-land.svg)](https://pypi.python.org/pypi/digital-land/)
[![Build](https://github.com/digital-land/pipeline/workflows/Digital-Land%20Continuous%20Integration/badge.svg)]
[![Coverage](https://coveralls.io/repos/github/digital-land/pipeline/badge.svg?branch=master)](https://coveralls.io/github/digital-land/pipeline?branch=master)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/digital-land/pipeline/blob/master/LICENSE)
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

Development requires Python 3.6 or later, we recommend using a [virtual environment](https://docs.python.org/3/library/venv.html):

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
