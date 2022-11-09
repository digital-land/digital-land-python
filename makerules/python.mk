PIP_INSTALL_PACKAGE=[test]

all:: lint test coverage

lint:: black-check flake8

black-check:
	black --diff .
	black --check .

black:
	black .

flake8:
	flake8 .

test:: test-unit test-integration test-e2e

test-unit:
	[ -d tests/unit ] && python -m pytest tests/unit --junitxml=.junitxml/unit.xml

test-integration:
	[ -d tests/integration ] && python -m pytest tests/integration --junitxml=.junitxml/integration.xml

test-e2e:
	[ -d tests/e2e ] && python -m pytest tests/e2e --junitxml=.junitxml/e2e.xml

coverage:
	coverage run --source $(PACKAGE) -m pytest && coverage report

coveralls:
	py.test --cov $(PACKAGE) tests/ --cov-report=term --cov-report=html

bump::
	git tag $(shell python version.py)

dist:: all
	python setup.py sdist bdist_wheel

upload::	dist
	twine upload dist/*

makerules::
	curl -qfsL '$(SOURCE_URL)/makerules/main/python.mk' > makerules/python.mk
