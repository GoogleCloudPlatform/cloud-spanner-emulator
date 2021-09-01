#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash

if [[ "$#" -eq 0 ]]; then
  echo "Running client library integration tests."
  DOCKER_ARGS="";
  DOCKER_CMD="build/kokoro/gcp_ubuntu/build.sh"
elif [[ "$#" -eq 1 ]] && [[ "$1" = "--debug" ]]; then
  echo "Launching in integration test debug environment."
  # If '--debug' is specified, the script launches an interactive shell in the
  # integration test environment. This is useful for debugging integration test
  # failures. Tests can be run by setting the CLIENT_INTEGRATION_TESTS
  # environment variable and running the build.sh
  DOCKER_ARGS="-it"
  DOCKER_CMD=""
else
  echo "Invalid arguments to script: $@"
  echo "Usage: docker_test.sh [--debug]"
fi

# Fail on any error.
set -e

# Start echoing all the commands so they show in the log.
set -x

declare OUTPUT_DIR=""
declare LOG_DIR=""
declare SRC_DIR=""
# Account for in-Kokoro or not.
if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
  # Switch to source root.
  SRC_DIR="${KOKORO_ARTIFACTS_DIR}/git/cloud-spanner-emulator"
  cd $SCR_DIR
  # On kokoro, script runs inside a Docker container as root, so $HOME
  # should point to the correct config.
  GCLOUD_CONFIG_DIR=${HOME}/.config/gcloud
  OUTPUT_DIR="${KOKORO_ARTIFACTS_DIR}"
  # Create test log directory.
  LOG_DIR="${KOKORO_ARTIFACTS_DIR}/logs"
  mkdir -p "${LOG_DIR}"
else
  # If running locally, this should be executed from the cloud-spanner-emulator
  # directory.
  SRC_DIR=$PWD
  # When running locally, you may have to use Docker with sudo, in which case,
  # the non-sudo user's gcloud config should be used, instead of the root
  # users's gcloud config.
  GCLOUD_CONFIG_DIR=$(eval echo ~$(logname))/.config/gcloud
  OUTPUT_DIR=$(mktemp -d)
  # Create test log directory.
  LOG_DIR=$(mktemp -d)
fi
echo "Placing results in: ${OUTPUT_DIR}"
echo "Placing logs in: ${LOG_DIR}"
echo "Using src directory: ${SRC_DIR}"

readonly CONTINUOUS_INTEGRATION_DOCKER_IMAGE=gcr.io/cloud-spanner-emulator-builder/build-integration:20210823
readonly BASE_DOCKER_IMAGE=gcr.io/cloud-spanner-emulator-builder/build-base:20210704
if [[ -z "$CLIENT_INTEGRATION_TESTS" ]]; then
  DOCKER_IMAGE=${BASE_DOCKER_IMAGE}
else
  # Use custom docker image for continuous integration tests.
  DOCKER_IMAGE=${CONTINUOUS_INTEGRATION_DOCKER_IMAGE}
fi
readonly CACHE_BUCKET="cloud-spanner-emulator-builder-bazel-cache"
readonly REMOTE_CACHE="https://storage.googleapis.com/${CACHE_BUCKET}/${DOCKER_IMAGE}"
echo "Using docker image: ${DOCKER_IMAGE}"
echo "Using bazel remote cache: ${REMOTE_CACHE}"
echo "Client integration tests to run: ${CLIENT_INTEGRATION_TESTS}"

# Activate our GCloud credentials with docker to allow GCR access.
yes | gcloud auth configure-docker

echo "--------------------------------------------------------------------"
docker run $DOCKER_ARGS \
  --env CC="/usr/bin/gcc" \
  --env CXX="/usr/bin/g++" \
  --env GCLOUD_DIR="/usr/local/gcloud/google-cloud-sdk/bin" \
  --env GO_BINARY_DIR="linux_amd64_stripped" \
  --env CLIENT_LIB_DIR="/root/clients" \
  --env COPY_LOGS_TO="/logs" \
  --env REMOTE_CACHE="${REMOTE_CACHE}" \
  --env KOKORO_ARTIFACTS_DIR="${KOKORO_ARTIFACTS_DIR}" \
  --env CLIENT_INTEGRATION_TESTS="${CLIENT_INTEGRATION_TESTS}" \
  --env EMULATOR_SRC_DIR="/src" \
  --volume "${GCLOUD_CONFIG_DIR}:/root/.config/gcloud" \
  --volume "${SRC_DIR}:/src" \
  --volume "${LOG_DIR}:/logs" \
  --workdir "/src" \
  --entrypoint="/bin/bash" \
  ${DOCKER_IMAGE} ${DOCKER_CMD}

