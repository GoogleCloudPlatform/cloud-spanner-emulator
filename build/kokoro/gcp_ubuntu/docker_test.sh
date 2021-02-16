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
  OUTPUT_DIR="${KOKORO_ARTIFACTS_DIR}"
  # Create test log directory.
  LOG_DIR="${KOKORO_ARTIFACTS_DIR}/log"
  mkdir -p "${LOG_DIR}"
else
  # If running locally, this should be executed from the cloud-spanner-emulator
  # directory.
  SRC_DIR=$PWD
  OUTPUT_DIR=$(mktemp -d)
  # Create test log directory.
  LOG_DIR=$(mktemp -d)
fi
echo "Placing results in: ${OUTPUT_DIR}"
echo "Placing logs in: ${LOG_DIR}"
echo "Using src directory: ${SRC_DIR}"

readonly DOCKER_IMAGE=gcr.io/cloud-spanner-emulator-builder/build-base:20201215
readonly CACHE_BUCKET="cloud-spanner-emulator-builder-bazel-cache"
readonly REMOTE_CACHE="https://storage.googleapis.com/${CACHE_BUCKET}/${DOCKER_IMAGE}"
echo "Using docker image: ${DOCKER_IMAGE}"
echo "Using bazel remote cache: ${REMOTE_CACHE}"
echo "Client integration tests to run: ${CLIENT_INTEGRATION_TESTS}"

# Activate our GCloud credentials with docker to allow GCR access.
yes | gcloud auth configure-docker

echo "--------------------------------------------------------------------"
docker run \
  --env CC="/usr/bin/gcc" \
  --env CXX="/usr/bin/g++" \
  --env GCLOUD_DIR="/usr/local/gcloud/google-cloud-sdk/bin" \
  --env GO_BINARY_DIR="linux_amd64_stripped" \
  --env CLIENT_LIB_DIR="/root/clients" \
  --env REMOTE_CACHE="${REMOTE_CACHE}" \
  --env KOKORO_ARTIFACTS_DIR="${KOKORO_ARTIFACTS_DIR}" \
  --env CLIENT_INTEGRATION_TESTS="${CLIENT_INTEGRATION_TESTS}" \
  --env EMULATOR_SRC_DIR="/src" \
  --volume "$HOME/.config/gcloud:/root/.config/gcloud" \
  --volume "${SRC_DIR}:/src" \
  --volume "${LOG_DIR}:/logs" \
  --workdir "/src" \
  --entrypoint="/bin/bash" \
  ${DOCKER_IMAGE} \
  build/kokoro/gcp_ubuntu/build.sh
