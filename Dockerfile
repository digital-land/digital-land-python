FROM python:3.8 as digital-land-python

RUN set -xe && \
    apt-get update && \
    apt-get install -y \
        gosu \
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
RUN pip install --upgrade pip
RUN pip install virtualenv
RUN cd /opt && virtualenv venv --always-copy
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN make init

FROM digital-land-python as digital-land-specification

RUN mkdir -p /collection
WORKDIR /collection
RUN set -ex; \
    wget https://github.com/digital-land/makerules/archive/refs/heads/run-pipeline-commands-locally.zip -O makerules.zip; \
    unzip makerules.zip; \
    make specification -f makerules-run-pipeline-commands-locally/makerules.mk
# TODO switch branch when merged
# wget https://github.com/digital-land/makerules/archive/refs/heads/main.zip -O makerules.zip; \
# make specification -f makerules-main/makerules.mk

FROM digital-land-specification as digital-land-pipeline

# Approach borrowed from https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
COPY docker_entrypoint.sh /usr/local/bin/docker_entrypoint.sh
RUN chmod +x /usr/local/bin/docker_entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker_entrypoint.sh"]

WORKDIR /pipeline
