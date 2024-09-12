#!/bin/bash
set -euxo pipefail

# Runs a basic e2e test. It is not meant to be fully comprehensive,
# but rather should catch obvious bugs before they make it into prod.
#
# Script assumptions:
#   - The gradle build has already been run.
#   - Python 3.6+ is installed and in the PATH.

# Log the locally loaded images
# docker images | grep "datahub-"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

if [ "${RUN_QUICKSTART:-true}" == "true" ]; then
    source ./run-quickstart.sh
else
  mkdir -p ~/.datahub/plugins/frontend/auth/
  echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props
  echo "admin:mypass" > ~/.datahub/plugins/frontend/auth/user.props

  python3 -m venv venv
  source venv/bin/activate
  python -m pip install --upgrade pip uv>=0.1.10 wheel setuptools
  uv pip install -r requirements.txt
fi

# set environment variables for the test
export DATAHUB_GMS_URL=http://localhost:8080

# source ./set-test-env-vars.sh
pytest -rP --durations=20 -vv --continue-on-collection-errors --junit-xml=junit.smoke.xml
