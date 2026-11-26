# deduce the repository
ifeq ($(REPOSITORY),)
REPOSITORY=$(shell basename -s .git `git config --get remote.origin.url`)
endif

ifeq ($(ENVIRONMENT),)
ENVIRONMENT=production
endif

ifeq ($(SOURCE_URL),)
SOURCE_URL=https://raw.githubusercontent.com/digital-land/
endif

ifeq ($(MAKERULES_URL),)
MAKERULES_URL=$(SOURCE_URL)makerules/main/
endif

ifeq ($(DATASTORE_URL),)
DATASTORE_URL=https://files.planning.data.gov.uk/
endif

ifeq ($(CONFIG_URL),)
CONFIG_URL=$(DATASTORE_URL)config/
endif

ifeq ($(SPECIFICATION_URL),)
SPECIFICATION_URL=$(DATASTORE_URL)specification/
endif

ifeq ($(COLLECTION_NAME),)
COLLECTION_NAME=$(shell echo "$(REPOSITORY)"|sed 's/-collection$$//')
endif

ifeq ($(VAR_DIR),)
VAR_DIR=var/
endif

ifeq ($(CACHE_DIR),)
CACHE_DIR=$(VAR_DIR)cache/
endif


.PHONY: \
	makerules\
	specification\
	config\
	init\
	first-pass\
	second-pass\
	third-pass\
	clobber\
	clean\
	commit-makerules\
	prune

# keep intermediate files
.SECONDARY:

# don't keep targets build with an error
.DELETE_ON_ERROR:

# work in UTF-8
LANGUAGE := en_GB.UTF-8
LANG := C.UTF-8

# for consistent collation on different machines
LC_COLLATE := C.UTF-8

# current git branch
ifeq ($(BRANCH),)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
endif

UNAME := $(shell uname)

# detect location of spatialite library, for linux add to path so python can pick up thhe files
ifndef SPATIALITE_EXTENSION
ifeq ($(UNAME), Linux)
SPATIALITE_EXTENSION="/usr/lib/x86_64-linux-gnu/mod_spatialite.so"
endif
ifeq ($(UNAME), Darwin)
SPATIALITE_EXTENSION="/usr/local/lib/mod_spatialite.dylib"
endif
endif

all:: first-pass second-pass third-pass

first-pass::
	@:

# restart the make process to pick-up collected files
second-pass::
	@:

third-pass::
	@:

# initialise
init::
	pip install --upgrade pip
ifneq (,$(wildcard requirements.txt))
	pip3 install --upgrade -r requirements.txt
endif
ifneq (,$(wildcard pyproject.toml))
	pip install -e .$(PIP_INSTALL_PACKAGE)
endif
ifneq (,$(wildcard setup.py))
	pip install -e .$(PIP_INSTALL_PACKAGE)
endif
	sqlite3 --version

submodules::
	git submodule update --init --recursive --remote

# remove targets, force relink
clobber::
	@:

# remove intermediate files
clean::
	@:

# prune back to source code
prune::
	rm -rf ./$(VAR_DIR) $(VALIDATION_DIR)

# update makerules from source
makerules::
	curl -qfsL '$(MAKERULES_URL)makerules.mk' > makerules/makerules.mk

ifeq (,$(wildcard ./makerules/specification.mk))
# update local copies of specification files (S3 in AWS, CDN locally — same as organisation.csv)
SPECIFICATION_CSVS=\
	attribution licence typology theme collection dataset dataset-field \
	field datatype prefix provision-rule pipeline dataset-schema schema schema-field

specification:: $(addprefix specification/,$(addsuffix .csv,$(SPECIFICATION_CSVS)))

specification/%.csv:
	@mkdir -p specification/
ifneq ($(COLLECTION_DATASET_BUCKET_NAME),)
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/specification/$(notdir $@) $@ --no-progress
else
	curl -qfsL '$(SPECIFICATION_URL)$(notdir $@)?version=$(shell date +%s)' -o $@
endif

init::	specification
endif

# local copy of organsiation datapackage
$(CACHE_DIR)organisation.csv:
	@mkdir -p $(CACHE_DIR)
ifneq ($(COLLECTION_DATASET_BUCKET_NAME),)
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/organisation-collection/dataset/organisation.csv $(CACHE_DIR)organisation.csv
else
	curl -qfs "$(DATASTORE_URL)organisation-collection/dataset/organisation.csv" > $(CACHE_DIR)organisation.csv
endif

init:: config

config::;

commit-makerules::
	git add makerules
	git diff --quiet && git diff --staged --quiet || (git commit -m "Updated makerules $(shell date +%F)"; git push origin $(BRANCH))

commit-collection::
	@:
