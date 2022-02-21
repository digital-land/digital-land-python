PACKAGE=digital_land

include makerules/makerules.mk
include makerules/python.mk

GDAL := $(shell command -v ogr2ogr 2> /dev/null)
SQLDIFF := $(shell command -v sqldiff 2> /dev/null)
UNAME := $(shell uname)

# install dependencies
init::
ifndef GDAL
ifeq ($(UNAME),Darwin)
$(error GDAL tools not found in PATH)
endif
	sudo apt-get install gdal-bin
endif
ifndef SQLDIFF
ifeq ($(UNAME),Darwin)
$(error sqldiff not found in PATH)
endif
	sudo apt-get install sqlite3
endif
