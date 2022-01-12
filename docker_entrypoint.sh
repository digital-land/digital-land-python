#!/bin/bash

# Add local user
# Either use the LOCAL_USER_ID if passed in at runtime or
# fallback to $UID

USER_ID=${LOCAL_USER_ID:-$UID}

echo "Starting with UID : $USER_ID"
useradd --shell /bin/bash -u $USER_ID -o -c "" -m user
export HOME=/home/user

# Docker will create any new local folders on host specified in bind mounts as being owned by root. Fix this
chown -R $USER_ID /pipeline

# Install packages from the collection repository
[[ -f /pipeline/requirements.txt ]] && /opt/venv/bin/pip install --upgrade -r requirements.txt
[[ -f /pipeline/setup.py ]] && /opt/venv/bin/pip install -e ".${PIP_INSTALL_PACKAGE:-test}"
/opt/venv/bin/pip install csvkit
# make init -f /collection/makerules-main/makerules.mk
# # TODO switch branch when merged
# # make init -f /collection/makerules-run-pipeline-commands-locally/makerules.mk

exec gosu user "$@"

