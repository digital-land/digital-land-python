.PHONY: all black test coverage coveralls bump dist upload help clean

all:	black test coverage

test:
	py.test

coverage:
	coverage run --source digital_land -m py.test && coverage report

coveralls:
	py.test --cov digital_land tests/ --cov-report=term --cov-report=html

black:
	black .

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
