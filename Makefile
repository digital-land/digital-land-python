PACKAGE=digital_land

include makerules/makerules.mk
include makerules/python.mk

GDAL := $(shell command -v ogr2ogr 2> /dev/null)
SQLDIFF := $(shell command -v sqldiff 2> /dev/null)
UNAME := $(shell uname)

ifeq ($(UNAME),Darwin)
	SPATIAL := $(shell ls /usr/local/lib/*spatialite* 2> /dev/null)
else
	SPATIAL := $(shell ls /usr/lib/x86_64-linux-gnu/*spatialite* 2> /dev/null)
endif

# install dependencies
init::
ifndef GDAL
ifeq ($(UNAME),Darwin)
	$(error GDAL tools not found in PATH)
endif
	sudo add-apt-repository ppa:ubuntugis/ppa
	sudo apt-get update
	sudo apt-get install gdal-bin
	gdalinfo --version
endif
ifndef SQLDIFF
ifeq ($(UNAME),Darwin)
	$(error sqldiff not found in PATH)
endif
	sudo apt-get install sqlite3
endif
ifndef SPATIAL
ifeq ($(UNAME),Darwin)
	$(error GDAL tools not found in PATH)
endif
	sudo apt-get install libsqlite3-mod-spatialite -y
endif
	pyproj sync --file uk_os_OSTN15_NTv2_OSGBtoETRS.tif -v
# install pre-commits
	pre-commit install

# Build documentation with Sphinx use for deploying documentation
.PHONY: docs
docs: api-docs
	sphinx-apidoc -o docs/ digital_land
	sphinx-build -b html docs/ docs/_build

# Serve the documentation locally with live-reloading. Use this for local building
serve-docs:
	sphinx-apidoc -o docs/ digital_land
	sphinx-autobuild docs/ docs/_build --host 0.0.0.0 --port 8000

# Clean up the generated documentation
clean::
	rm -rf docs/_build