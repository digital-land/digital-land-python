# Digital Land Pipeline

[![Continuous Integration](https://github.com/digital-land/digital-land-python/actions/workflows/continuous-integration.yml/badge.svg)](https://github.com/digital-land/digital-land-python/actions/workflows/continuous-integration.yml)
[![codecov](https://codecov.io/gh/digital-land/digital-land-python/branch/main/graph/badge.svg?token=IH2ETPF2C1)](https://codecov.io/gh/digital-land/digital-land-python)
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
      -n, --dataset TEXT
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
      dataset-entries                dump dataset entries as csv
      fetch                          fetch resource from a single endpoint
      pipeline                       process a resource
      add-endpoint-and-lookups       add batch of endpoints from csv

## Development environment

The GDAL tools are required to convert geographic data, and in order for all of the tests to pass.

Makefile depends on GNU make if using macOS install make using brew and run gmake.  

Development requires Python 3.6.2 or later, we recommend using a [virtual environment](https://docs.python.org/3/library/venv.html):

    make init
    make
    python -m digital-land --help


## Commands Guide

### add-endpoint-and-lookups

#### 1. Checkout repo
From Github, checkout the Collection repo you wish to add endpoints to.
Make a note of the location [COLL-DIR].

#### 2. run collection init
Run the following lines from a virtual env containing the required
python dependencies required by the collection repo
```
make makerules
make init
```

#### 3. get your csv of endpoints to add
Find or generate a csv file of the entries that need to be added
to the collection. Make a note of the location [CSV-PATH/file.csv].

#### 4. run add-endpoint-and-lookups
Run the following line from a virtual env containing the required
python dependencies required by digital-land-python
(line broken up for readability)
```
-n [DATASET-NAME] \ 
-p [COLL-DIR]/pipeline \
-s [SPECIFICATION-DIR] \
add-endpoint-and-lookups \
[CSV-PATH/new_endpoints.csv] \
-c [COLL-DIR]/collection
```

#### 5. check assigned entities are normal
After running the command, the following Collection repo
files will be modified:

```
collection/source.csv
collection/endpoints.csv
pipeline/lookup.csv
```

The console output will show a list of new lookup entries
organised by organisation and resource-hash.
E.g.
```
----------------------------------------------------------------------
>>> organisations:['local-authority-eng:ARU']
>>> resource:6c38cd1f84054051ca200d62e9715be0cd739bedbae0db9561ef091fa95f59f1
----------------------------------------------------------------------
brownfield-land,,local-authority-eng:ARU,BR23911,1729345
brownfield-land,,local-authority-eng:ARU,BR19811,1729346
...
```
Find the first lookup entry in the console output, 
make a note of the entity id (the number at the end),
and find this in pipeline/lookup.csv.

Check this and all subsequent lines in lookup.csv for
any anomalies. Each record should have, as a minimum, a prefix,
organisation and reference.

#### 6. run pipeline
Run the following line from a virtual env containing the required
python dependencies required by the collection repo
```
make collect
```

#### 7. check final datasette for any weirdness


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


