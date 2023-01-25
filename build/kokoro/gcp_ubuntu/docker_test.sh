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
  DOCKER_CMD_INTEGRATION_TESTS="build/kokoro/gcp_ubuntu/integration_tests.sh"
  DOCKER_CMD_BUILD_AND_TEST="build/kokoro/gcp_ubuntu/build_and_test.sh"
elif [[ "$#" -eq 1 ]] && [[ "$1" = "--debug" ]]; then
  echo "Launching in integration test debug environment."
  # If '--debug' is specified, the script launches an interactive shell in the
  # integration test environment. This is useful for debugging integration test
  # failures. Tests can be run by setting the CLIENT_INTEGRATION_TESTS
  # environment variable and running the build.sh
  DOCKER_ARGS="-it"
  DOCKER_CMD_INTEGRATION_TESTS=""
  DOCKER_CMD_BUILD_AND_TEST=""
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

readonly BASE_DOCKER_IMAGE=gcr.io/cloud-spanner-emulator-builder/build-base@sha256:c58f9b2f008d1a14370ddadf022c3c2dea1d924eb534820e427fa53a325d48e1
readonly CACHE_BUCKET="cloud-spanner-emulator-builder-bazel-cache"
echo "Client integration tests to run: ${CLIENT_INTEGRATION_TESTS}"

# Activate our GCloud credentials with docker to allow GCR access.
yes | gcloud auth configure-docker

echo "--------------------------------------------------------------------"
if [[ "$#" -eq 0 ]]; then
REMOTE_CACHE="https://storage.googleapis.com/${CACHE_BUCKET}/${BASE_DOCKER_IMAGE}"
container_id=$(docker create $DOCKER_ARGS \
    --env CC="/usr/bin/gcc" \
    --env CXX="/usr/bin/g++" \
    --env GCLOUD_DIR="/usr/local/gcloud/google-cloud-sdk/bin" \
    --env CLIENT_LIB_DIR="/root/clients" \
    --env COPY_LOGS_TO="/logs" \
    --env REMOTE_CACHE="${REMOTE_CACHE}" \
    --env KOKORO_ARTIFACTS_DIR="${KOKORO_ARTIFACTS_DIR}" \
    --env EMULATOR_SRC_DIR="/src" \
    --volume "${GCLOUD_CONFIG_DIR}:/root/.config/gcloud" \
    --volume "${SRC_DIR}:/src" \
    --volume "${LOG_DIR}:/logs" \
    --workdir "/src" \
    --entrypoint="/bin/bash" \
    ${BASE_DOCKER_IMAGE} ${DOCKER_CMD_BUILD_AND_TEST})
docker start -a "$container_id"
docker wait "$container_id"
docker cp "$container_id":/src/bazel-bin/binaries/gateway_main_/gateway_main ${SRC_DIR}
docker cp "$container_id":/src/bazel-bin/binaries/emulator_main ${SRC_DIR}
docker rm "$container_id"
fi

IFS=','
for client in $CLIENT_INTEGRATION_TESTS
 do
    if [[ $client == "go" ]]; then
      SHA=d2d8b4d15d7614c4f84623593c6e8b7d1e4207bb1ad2007b936d1004273c7337
    elif [[ $client == "java" ]]; then
      SHA=7e61402fa1d952c4f352e122590c48ae75a73d296eb32e1aed07bae1715e78c7
    elif [[ $client == "cpp" ]]; then
      SHA=28b68ebfa796b2a26a63968f39a14f81e46cb3ec8364098f84870332a003787f
    elif [[ $client == "php" ]]; then
      SHA=d697883a79b01b69792f91f2fcdc931982bab354671e5e57ecd4ba9c7b89c08a
    elif [[ $client == "csharp" ]]; then
      SHA=6ee4f71c147b35f6013fbfd3c1e5b9624cc4db4f4e996ce39480da69ca0b272a
    elif [[ $client == "ruby" ]]; then
      SHA=0c728d91a3520c20fb0d487447590da558638246df66ef06839e13ee442d6368
    elif [[ $client == "nodejs" ]]; then
      SHA=9bad1aab76f336ed565235cee19ee1fd69f37665004901d041267809eac2c572
    elif [[ $client == "py" ]]; then
      SHA=5148dc8858fc2a6302d3477ace37e4492ee24a219bfa88126367faaeb7e149d5
    else
    echo "Unrecognized client: \"${client}\"."
    fi
    DOCKER_IMAGE=gcr.io/cloud-spanner-emulator-builder/build-integration-${client}@sha256:${SHA}
    docker run $DOCKER_ARGS \
      --env CC="/usr/bin/gcc" \
      --env CXX="/usr/bin/g++" \
      --env GCLOUD_DIR="/usr/local/gcloud/google-cloud-sdk/bin" \
      --env CLIENT_LIB_DIR="/root/clients" \
      --env COPY_LOGS_TO="/logs" \
      --env KOKORO_ARTIFACTS_DIR="${KOKORO_ARTIFACTS_DIR}" \
      --env CLIENT_INTEGRATION_TESTS="${client}" \
      --env EMULATOR_SRC_DIR="/src" \
      --volume "${GCLOUD_CONFIG_DIR}:/root/.config/gcloud" \
      --volume "${SRC_DIR}:/src" \
      --volume "${LOG_DIR}:/logs" \
      --workdir "/src" \
      --entrypoint="/bin/bash" \
      ${DOCKER_IMAGE} ${DOCKER_CMD_INTEGRATION_TESTS}
 done
