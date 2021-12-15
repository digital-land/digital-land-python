FROM python:3.8

RUN set -xe && \
    apt-get update && \
    apt-get install -y \
        make \
        git \
        gdal-bin \
        libspatialite-dev \
        libsqlite3-mod-spatialite

WORKDIR /src
COPY . /src
RUN make init
