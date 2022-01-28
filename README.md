# Digital Land Pipeline

[![Build](https://github.com/digital-land/pipeline/workflows/Continuous%20Integration/badge.svg)](https://github.com/digital-land/pipeline/actions?query=workflow%3A%22Continuous+Integration%22)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/digital-land/digital-land-python/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://black.readthedocs.io/en/stable/)


*Python command-line tools for collecting and converting resources into a dataset*

## Installation

    pip3 install digital-land

## Command line

    $ digital-land --help

    Usage: digital-land [OPTIONS] COMMAND [ARGS]...

    Options:
      -d, --debug / --no-debug
      -n, --pipeline-name TEXT
      -p, --pipeline-dir PATH
      -s, --specification-dir PATH
      --help                        Show this message and exit.

    Commands:
      build-datasette                build docker image for datasette
      collect                        fetch resources from collection endpoints
      collection-add-source          Add a new source and endpoint to a collection
      collection-check-endpoints     check logs for failing endpoints
      collection-list-resources      list resources for a pipeline
      collection-pipeline-makerules  generate pipeline makerules for a collection
      collection-save-csv            save collection as CSV package
      convert                        convert a resource to CSV
      dataset-create                 create a dataset from processed resources
      dataset-entries                create entries from dataset facts
      fetch                          fetch resource from a single endpoint
      filter                         remove unnecessary rows
      harmonise                      harmonise field values according to its
                                     datatype
      map                            map column names to dataset fields
      normalise                      remove whitespace and empty rows
      pipeline                       process a resource
      transform                      migrate field names to the latest
                                     specification

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
