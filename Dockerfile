FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && \
    apt-get install git curl build-essential software-properties-common sudo -y


RUN add-apt-repository ppa:ubuntugis/ppa -y && \
    apt-get update -y && \
    apt-get install libpq-dev gdal-bin libgdal-dev sqlite3 libsqlite3-mod-spatialite -y


RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update -y && \
    apt-get install python3.9 python3-pip python-is-python3 -y

RUN python -m pip install spatialite pyyaml dataclasses-json


COPY . /src
WORKDIR /src

RUN make makerules && make init