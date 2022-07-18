PIP_INSTALL_PACKAGE=[test]
ECR_REPO=public.ecr.aws/l6z6v3j6/

LAYER_NAME=digital-land-python
REPO_NAME=digital-land-python
ECR_PATH=$(ECR_REPO)$(REPO_NAME):$(LAYER_NAME)

all:: lint test coverage

lint:: black-check flake8

black-check:
	black --diff .
	black --check .

black:
	black .

flake8:
	flake8 .

test:: test-unit test-integration test-e2e test-airflow

test-unit:
	[ -d tests/unit ] && python -m pytest tests/unit --junitxml=.junitxml/unit.xml

test-integration:
	[ -d tests/integration ] && python -m pytest tests/integration --junitxml=.junitxml/integration.xml

test-e2e:
	[ -d tests/e2e ] && python -m pytest tests/e2e --junitxml=.junitxml/e2e.xml

test-airflow::
	python -m pytest $(shell python -c "import inspect, os; from digital_land_airflow import tests; print(os.path.dirname(inspect.getfile(tests)))") -p digital_land_airflow.tests.fixtures.base --junitxml=.junitxml/airflow.xml

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
	docker build . -t $(ECR_PATH) --target $(LAYER_NAME)

docker-push: docker-check docker-ecr-login
	docker push $(ECR_PATH)

docker-pull: docker-check docker-ecr-login
	docker pull $(ECR_PATH)

docker-shell: docker-check docker-ecr-login
	docker run -it $(ECR_PATH) bash

docker-check:
ifeq (, $(shell which docker))
	$(error "No docker in $(PATH), consider doing apt-get install docker OR brew install --cask docker")
endif

docker-ecr-login:
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
