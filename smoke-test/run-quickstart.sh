#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

../gradlew :smoke-test:installDev
source venv/bin/activate

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

echo "DATAHUB_ACTIONS_VERSION = $DATAHUB_ACTIONS_VERSION"

DATAHUB_TELEMETRY_ENABLED=false  \
ACTIONS_VERSION=${DATAHUB_ACTIONS_VERSION}  \
datahub docker quickstart