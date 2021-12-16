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

# Necessary to make sure the `digital-land` easyinstall entry script is found when invoking
# `digital-land` via shell from python space
ENV VIRTUAL_ENV=/opt/venv
RUN pip install --update pip
RUN pip install virtualenv
RUN cd /opt && virtualenv venv --always-copy
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN make init

