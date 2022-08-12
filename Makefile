PACKAGE=digital_land

include makerules/makerules.mk
include makerules/python.mk

SPACIAL := $(shell command -v mod_spatialite 2> /dev/null)
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
ifndef SPACIAL
ifeq ($(UNAME),Darwin)
$(error GDAL tools not found in PATH)
endif
	apt-get install libsqlite3-mod-spatialite -y
endif
	pyproj sync --file uk_os_OSTN15_NTv2_OSGBtoETRS.tif -v
