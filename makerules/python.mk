PIP_INSTALL_PACKAGE=[test]
ECR_REPO=public.ecr.aws/l6z6v3j6/

all:: lint test coverage

lint:: black-check flake8

black-check:
	black --check .

black:
	black .

flake8:
	flake8 .

test:: test-unit test-integration test-e2e

test-unit:
	[ -d tests/unit ] && python -m pytest tests/unit

test-integration:
	[ -d tests/integration ] && python -m pytest tests/integration

test-e2e:
	[ -d tests/e2e ] && python -m pytest tests/e2e

coverage:
	coverage run --source $(PACKAGE) -m py.test && coverage report

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

docker-build: docker-check
	docker build . -t $(ECR_REPO)digital-land-python:digital-land-python

docker-push: docker-check docker-ecr-login
	docker push $(ECR_REPO)digital-land-python:digital-land-python

docker-check:
ifeq (, $(shell which docker))
	$(error "No docker in $(PATH), consider doing apt-get install docker OR brew install --cask docker")
endif

docker-ecr-login:
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
