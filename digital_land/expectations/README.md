## Green Box Data Quality

This tool was developed to make easy to run Data Quality checks agains our pipelines.

It's development environment was a docker container with the setup exactly as described here:
https://github.com/digital-land/digital-land-docker-pipeline-runner

Dependencies not yet included in the docker setup:

    apt-get install libsqlite3-mod-spatialite -y
    pip install spatialite
    pip install pyyaml
    pip install dataclasses-json

At the moment unit tests cover expectations.py and most of the core.py, unit tests for some parts of core and main were left to be completed later.

Executing a yaml data quality file against a sqlite3 file is done the following way:

    digital-land expectations --results-path "expectations/results" --sqlite-dataset-path "/src/sharing_area/conservation-area-collection/dataset/conservation-area.sqlite3" --data-quality-yaml "/src/sharing_area/green-box-data-quality/expectations/conservation-area.yaml"

Note that ideally in prod --results-path should be changed to a dir of your choice.
