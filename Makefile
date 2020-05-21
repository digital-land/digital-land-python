.PHONY: all black test test-unit test-integration coverage coveralls bump dist upload help clean

all:	black flake8 test coverage

test:
	python -m pytest

test-unit:
	python -m pytest tests/unit

test-integration:
	python -m pytest tests/integration

coverage:
	coverage run --source digital_land -m py.test && coverage report

coveralls:
	py.test --cov digital_land tests/ --cov-report=term --cov-report=html

black:
	black .

flake8:
	flake8 .

bump:
	git tag $(shell python version.py)

dist: all
	python setup.py sdist bdist_wheel

upload:	dist
	twine upload dist/*

# install dependencies
init:
	pip install -e .[test]

help:
	python -m digital_land --help

clean:
	-find . -name "*.pyc" | xargs rm -f
	-find . -name "__pycache__" | xargs rm -rf
	-rm -rf dist
	-rm -rf build
	-rm -rf digital_land.egg-info
