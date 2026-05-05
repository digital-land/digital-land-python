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

Before Initialising you will need to:
- ensure GNU make is being used, if using macOS then it may need installing
- ensure python is available on the system, Development requires Python 3.6.2 or later, see [our guidance](https://digital-land.github.io/technical-documentation/development/how-to-guides/using-different-python-versions/)
- set up a [virtual environment](https://docs.python.org/3/library/venv.html), see [our guidance](https://digital-land.github.io/technical-documentation/development/how-to-guides/make-python-venv/)
- ensure SQLite is installed and is capable of loading extensions

The GDAL tools are required to convert geographic data, and in order for all of the tests to pass.

after the above is satisfied run the following to get setup:

    make init
    python -m digital-land --help

On Linux this will automatically install key dependencies, on macOS or other systems it may error:
- The GDAL tools are required to convert geographic data, and in order for all of the tests to pass. see [our guidance](https://digital-land.github.io/technical-documentation/development/how-to-guides/installing-gdal/)

## Testing

> [!WARNING]
> Some machines may experience segmentation faults when running the test suite. This is a known issue.

This repository follows a structured testing approach with comprehensive test coverage across unit, integration, acceptance, and performance tests. See [TESTING.md](TESTING.md) for detailed testing guidelines and structure documentation.

### Test Structure

The test suite is organized into several categories:

- **Unit Tests** (`tests/unit/`) - Test individual components in isolation
- **Integration Tests** (`tests/integration/`) - Test component interactions
- **Acceptance Tests** (`tests/acceptance/`) - End-to-end workflow validation
- **Performance Tests** (`tests/performance/`) - Performance benchmarking

### Quick Test Commands

```bash
# Run all tests
make test

# Run specific test categories
pytest tests/unit/                    # Unit tests only
pytest tests/integration/             # Integration tests only
pytest tests/acceptance/              # Acceptance tests only
pytest tests/performance/             # Performance tests only

# Run phase-specific tests
pytest tests/unit/phase/              # Legacy phase tests
pytest tests/unit/phase_polars/       # New Polars-based phase tests
pytest tests/integration/phase_polars/ # Polars integration tests

# Run with coverage reporting
pytest --cov=digital_land --cov-report=html
pytest --cov=digital_land --cov-report=term-missing

# Run specific test files
pytest tests/unit/test_pipeline.py
pytest tests/integration/phase_polars/test_performance_benchmark_multi.py

# Run tests with verbose output
pytest -v tests/unit/phase_polars/transform/

# Run tests matching a pattern
pytest -k "test_harmonise" tests/
```

### Performance Benchmarking

The repository includes comprehensive performance benchmarking tools:

```bash
# Run performance benchmarks
python tests/integration/phase_polars/test_performance_benchmark_multi.py

# Run specific benchmark with limited files
python tests/integration/phase_polars/test_performance_benchmark_multi.py --files 5

# Run benchmark with custom CSV directory
python tests/integration/phase_polars/test_performance_benchmark_multi.py --csv-dir path/to/csvs
```

### Test Dependencies

Ensure you have the required test dependencies installed:

```bash
pip install pytest pytest-cov pytest-mock
```

### Continuous Integration

Tests are automatically run on GitHub Actions for all pull requests. The CI pipeline includes:

- Unit tests across multiple Python versions
- Integration tests with real data
- Code coverage reporting
- Performance regression detection


## Commands Guide

### add-endpoint-and-lookups

This command allows for adding multiple endpoints and lookups for 
datasets within a given collection, driven by entries in a csv file.

Detailed instructions for running this command can be found in the Data Operations manual
within the MHCLG technical documentation repository.

**Use with caution**

(currently only successfully tested on Brownfield Land collection)


## Release procedure

Update the tagged version number:

    make bump

Build the wheel and egg files:

    make dist

Push to GitHub:

    git push && git push --tags

Wait for the [continuous integration tests](https://pypi.python.org/pypi/digital-land/) to pass and then upload to [PyPI](https://pypi.python.org/pypi/digital-land/):

    make upload

## Notebooks

notebooks have been added which contain code that could be useful when debugging the system. Currently Jupyter isn't installed as part of the dev environment so before running you may need to install:

```
pip install jupyterlab
```

The notebooks are as follows:

* debug_resource_transformation.ipynb - given a resource and a dataset this downloads the resource and relevant information to process the resource. This is very useful for replicating errors that occur in this step.

# Licence

The software in this project is open source and covered by the [LICENSE](LICENSE) file.

Individual datasets copied into this repository may have specific copyright and licensing, otherwise all content and data in this repository is [© Crown copyright](http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/copyright-and-re-use/crown-copyright/) and available under the terms of the [Open Government 3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.


