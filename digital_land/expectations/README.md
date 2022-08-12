## Green Box Data Quality

This tool was developed to make easy to run Data Quality checks agains our pipelines.

It's development environment was a docker container with the setup exactly as described here:
https://github.com/digital-land/digital-land-docker-pipeline-runner

Dependencies not yet included in the docker setup:

    apt-get install libsqlite3-mod-spatialite -y
    pip install spatialite
    pip install pyyaml
    pip install dataclasses-json

The unit tests expect the tool to be running from the following dir inside the container:
    
    /src/sharing_area/green-box-data-quality/

To make it easier to run the unit tests we suggest:

    export PYTHONPATH=/src/sharing_area/green-box-data-quality/expectations/

This is a very early stage of the tool and several paths are hardcoded to their location inside the container mentioned above. This will be reviewed later.

Executing a suite of data quality tests yaml:

    digital-land expectations --results-path "expectations/results" --sqlite-dataset-path "/src/sharing_area/conservation-area-collection/dataset/conservation-area.sqlite3" --data-quality-yaml "/src/sharing_area/green-box-data-quality/expectations/conservation-area.yaml"

    