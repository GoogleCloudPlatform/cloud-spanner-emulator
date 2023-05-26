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
base_SHA=71038cf075f2c62edacd4ed7673770e79d6d5072924ec761b789bb817e602d60
cpp_SHA=043493e2a9f80039e2699d63784c455e8d8e34c82e3faaa6981f0ed68dfbb41c
csharp_SHA=c38cc0de3f8f7a2b54dda81b4c72cd5c673203a6d505879f71165709245350d0
go_SHA=25f586c611d6a223e38fae22e4a8eee8253824b043a16d6f553479612f01c36c
java_SHA=6880a6a88eaa7d4542657c02aaceb553823ab9add8d6f568673007d6109cfd39
nodejs_SHA=40144436e1d271e2b0e7411902ab094db42b21cad63d663f8ec8b3d4d02f08d9
php_SHA=c9dee8678e15e12df2f30ccc420f23331869d4fd8605661ae56bd1731c07a460
py_SHA=b331186119403fe921295dfa6096f0f2e6c9b308f50d7c53c56c6b7f93f1dd16
ruby_SHA=beaf6ac7f8de0d74494ccae93b78a55bb2703e66c0967afab49715ec7c8e826f

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
