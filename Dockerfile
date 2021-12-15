FROM python:3.8

RUN set -xe && \
    apt-get update && \
    apt-get install -y \
        make \
        git \
        gdal-bin \
        libspatialite-dev \
        libsqlite3-mod-spatialite

ENV PATH=/root/.local:$PATH

WORKDIR /src
COPY . /src

# Necessary to make sure the `digital-land` easyinstall entry script is found when invoking
# `digital-land` via shell from python space
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV --copies
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN make init

