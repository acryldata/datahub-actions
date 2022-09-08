
set -euo pipefail

cd ../acryl-executor
RELEASE_SKIP_UPLOAD=1 RELEASE_VERSION=0.0.0.dev0 ./scripts/release.sh
cd -
docker build .. -f docker/datahub-actions/Dockerfile -t acryldata/datahub-actions:debug
DATAHUB_VERSION=5d61924 ACTIONS_VERSION=debug ../../datahub/metadata-ingestion/venv/bin/datahub docker quickstart

