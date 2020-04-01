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

# Switch to source root so that version extraction and mock work.
if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
  # Switch to source root.
  cd ${KOKORO_ARTIFACTS_DIR}/git/cloud-spanner-emulator
fi

# Extract the version.
if [[ -f build/info/version.txt ]]; then
  while read -r line; do
    if [[ $line =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      EMULATOR_VERSION=$line
    fi
  done <build/info/version.txt
  if [[ -z "${EMULATOR_VERSION}" ]]; then
    echo "No version in build/info/version.txt" 1>&2
    exit 1
  fi
else
  EMULATOR_VERSION=0.0.0
fi

# Display commands being run.
set -x

# Skip the build if we are mocking.
if [[ "${CLOUD_SPANNER_EMULATOR_MOCK_BUILD}" == true ]]; then
  BIN_DIR="${KOKORO_ARTIFACTS_DIR:-/tmp}/bin"
  mkdir -p ${BIN_DIR}
  cp build/info/version.txt "${BIN_DIR}"
  tar -C "${BIN_DIR}" -czf "${BIN_DIR}/cloud-spanner-emulator-linux_amd64-${EMULATOR_VERSION}.tar.gz" version.txt
  exit 0
fi

# If running under kokoro, setup the environment.
if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
  # Use the version of bazel that is used in corp.
  use_bazel.sh 2.0.0

  # Install gcc-7 (current default is 4.8 which is too old).
  sudo add-apt-repository ppa:ubuntu-toolchain-r/test
  sudo apt-get -qq update
  sudo apt-get -qq install -y gcc-7 g++-7
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 90 \
                           --slave /usr/bin/g++ g++ /usr/bin/g++-7
  sudo update-alternatives --set gcc /usr/bin/gcc-7
fi

# Directory in which gcloud is located (gcloud should be accessible via PATH for
# this to work).
export GCLOUD_DIR=$(dirname $(which gcloud))

# Directory in which the go compiler places binaries in bazel.
export GO_BINARY_DIR=linux_amd64_stripped

# Run the build script.
set +e
./build/kokoro/build.sh
exit_code=${?}

set -e

# If running under kokoro, copy outputs to a predefined-dir
if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
  # Create binary and test log dirs.
  BIN_DIR="${KOKORO_ARTIFACTS_DIR}/bin"
  LOG_DIR="${KOKORO_ARTIFACTS_DIR}/log"
  mkdir -p "${BIN_DIR}"
  mkdir -p "${LOG_DIR}"

  # Copy test results.
  find -L . -name "test.log" -exec rename 's/test.log/sponge_log.log/' {} \;
  find -L . -name "sponge_log.log" -exec cp --parents {} "${LOG_DIR}" \;
  find -L . -name "test.xml" -exec rename 's/test.xml/sponge_log.xml/' {} \;
  find -L . -name "sponge_log.xml" -exec cp --parents {} "${LOG_DIR}" \;

  # Copy binaries.
  if [[ -f bazel-bin/binaries/emulator_main ]]; then
    cp bazel-bin/binaries/emulator_main "${BIN_DIR}"
  fi

  if [[ -f bazel-bin/binaries/linux_amd64_stripped/gateway_main ]]; then
    cp bazel-bin/binaries/linux_amd64_stripped/gateway_main "${BIN_DIR}"
  fi

  tar -C "${BIN_DIR}" -czf "${BIN_DIR}/cloud-spanner-emulator-linux_amd64-${EMULATOR_VERSION}.tar.gz" gateway_main emulator_main
fi

exit ${exit_code}
