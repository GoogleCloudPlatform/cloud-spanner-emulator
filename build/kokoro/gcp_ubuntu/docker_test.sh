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
base_SHA=efe211c89e88df39c3c8125613381176f67c96bf3e2a70525899cc274be8e7d3
cpp_SHA=ccbe18c26e5383c13cdccd2cc0cfe24e9fe61a791d99663addeb42bad32d3db0
csharp_SHA=e1b0998dc4b931bbb2f240af630610701963804aaf9b5ac5d59f2200d6255518
go_SHA=4f2e102f103d6d5e3ebd6fc5e1604515adbd5ef094798adab158221a1c26d84a
java_SHA=96c18094adda489fc77d3480bfa9aa4fe3115058948a88a0b349843c3fc04e35
nodejs_SHA=1cb1914d986e5e8261c272e752f88bed9002df4280f927579cbef1ccc791bdb0
php_SHA=c207a447edb1b3e73baf4538c175a16b43d4888024d256845c62c26ae3c48cde
py_SHA=6eee1276f3022c3090e4a11c018c8e8659d4c244fc30e3b58086054662a85563
ruby_SHA=e600c76963d35689bb64e736b2ac6fa461ecc7808625034221a636619b249474

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
    --env GCLOUD_DIR="/usr/bin" \
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
      --env GCLOUD_DIR="/usr/bin" \
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
