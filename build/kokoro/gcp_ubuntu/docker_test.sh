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
base_SHA=cf586d8ae09c4a0f9ba613f1fb89104b7f2f245729f88204bb409fba2266b33a
go_SHA=9ba9ca1bab302461de4d6462ed2a64a8b5fe5cabb8c718a7a31a25cfd4308187
java_SHA=5da0c7567fe3224cf8d686429de3f446159ef742f111b9d4aaf120ca7a4122e3
cpp_SHA=09b8c004e05174b913d9faed7e4bb3586a35349aed7782a1d16b9c36f075cbd4
php_SHA=83be858b5f16da1ae68584ab4b99d483a9c3235336ac2d5e4514b6376bac1f56
csharp_SHA=9840a6e57da7c7c0b3017752356b6b73822097cee46b930f3f22826f0ac7d560
ruby_SHA=aabd3e1acf9f5c7c4444b72db757c94ab6cb6c9ce716f4143a896dd808bff55b
nodejs_SHA=6b64712aa937c4c785533bb5c2fdd9a87e3fd4702643191e09094b41ea0281b4
py_SHA=8d46cba4c1bde2c976a7f6edf01d2eae534b4e1eafdbb72ce4f52f560a49d439

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

readonly BASE_DOCKER_IMAGE=gcr.io/cloud-spanner-emulator-builder/build-base@sha256:${base_SHA}
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
      SHA=$go_SHA
    elif [[ $client == "java" ]]; then
      SHA=$java_SHA
    elif [[ $client == "cpp" ]]; then
      SHA=$cpp_SHA
    elif [[ $client == "php" ]]; then
      SHA=$php_SHA
    elif [[ $client == "csharp" ]]; then
      SHA=$csharp_SHA
    elif [[ $client == "ruby" ]]; then
      SHA=$ruby_SHA
    elif [[ $client == "nodejs" ]]; then
      SHA=$nodejs_SHA
    elif [[ $client == "py" ]]; then
      SHA=$py_SHA
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
