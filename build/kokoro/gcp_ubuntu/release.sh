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

# Account for in-Kokoro or not.
if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
  # Switch to source root.
  cd "${KOKORO_ARTIFACTS_DIR}"/git/cloud-spanner-emulator
  OUTPUT_DIR="${KOKORO_ARTIFACTS_DIR}"
else
  OUTPUT_DIR=$(mktemp -d)
fi
echo "Placing results in ${OUTPUT_DIR}"


# Require release version.
if [[ -z "${CLOUD_SPANNER_EMULATOR_RELEASE_TAG}" ]]; then
  echo "No CLOUD_SPANNER_EMULATOR_RELEASE_TAG" 1>&2
  exit 1
fi

# Start echoing all the commands so they show in the log.
set -x
EMULATOR_VERSION="${CLOUD_SPANNER_EMULATOR_RELEASE_TAG}"

echo "Building version " "${EMULATOR_VERSION}"

# Create the Docker image that we want to include in the release.
# It will be tagged with emulator:${USER}-${EMULATOR_VERSION} and sitting locally.
IMAGE_LOCAL_TAG=emulator:${USER}-${EMULATOR_VERSION}


if [[ "${CLOUD_SPANNER_EMULATOR_MOCK_BUILD}" == true ]]; then
  docker build -t "${IMAGE_LOCAL_TAG}" -<<EOF
FROM busybox
RUN echo "hello world" > gateway_main && echo "hello world" > emulator_main
RUN chmod +x gateway_main emulator_main
CMD echo "Mocked Emulator"
EOF
else
  # Activate our GCloud credentials with docker to allow GCR access.
  yes | gcloud auth configure-docker
  docker build . -t "${IMAGE_LOCAL_TAG}" -f build/kokoro/gcp_ubuntu/Dockerfile.release
fi

# We need the image tar file to be in a directory of its own.
IMAGE_DIR=${OUTPUT_DIR}/image
mkdir -p "${IMAGE_DIR}"
docker save --output "${IMAGE_DIR}"/emulator-docker-image.tar ${IMAGE_LOCAL_TAG}

container_id=$(docker create "${IMAGE_LOCAL_TAG}")
docker cp "$container_id":/gateway_main $OUTPUT_DIR
docker cp "$container_id":/emulator_main $OUTPUT_DIR
docker rm "$container_id"

tar -C "${OUTPUT_DIR}" -czf "${OUTPUT_DIR}"/cloud-spanner-emulator_linux_amd64-"${EMULATOR_VERSION}".tar.gz gateway_main emulator_main

