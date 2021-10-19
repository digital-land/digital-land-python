.PHONY: all black black-check test test-unit test-integration coverage coveralls bump dist upload help clean

# work in UTF-8
LANGUAGE := en_GB.UTF-8
LANG := C.UTF-8

# for consistent collation on different machines
LC_COLLATE := C.UTF-8

all:	lint test coverage

test-unit:
	python -m pytest tests/unit

test-integration:
	python -m pytest tests/integration

test-end-to-end:
	python -m pytest tests/e2e

test: test-unit test-integration test-end-to-end

coverage:
	coverage run --source digital_land -m py.test && coverage report

coveralls:
	py.test --cov digital_land tests/ --cov-report=term --cov-report=html

black:
	black .

black-check:
	black --check .

flake8:
	flake8 .

lint: black-check flake8

bump:
	git tag $(shell python version.py)

dist: all
	python setup.py sdist bdist_wheel

upload:	dist
	twine upload dist/*

GDAL := $(shell command -v ogr2ogr 2> /dev/null)
UNAME := $(shell uname)

# install dependencies
init:
	pip install --upgrade pip
	pip install -e .[test]
	curl -qfsL 'https://truststore.pki.rds.amazonaws.com/eu-west-2/eu-west-2-bundle.pem' > ssl.pem
ifndef GDAL
ifeq ($(UNAME),Darwin)
$(error GDAL tools not found in PATH)
endif
	sudo apt-get install gdal-bin
endif

help:
	python -m digital_land --help

clean:
	-find . -name "*.pyc" | xargs rm -f
	-find . -name "__pycache__" | xargs rm -rf
	-rm -rf dist
	-rm -rf build
	-rm -rf digital_land.egg-info
