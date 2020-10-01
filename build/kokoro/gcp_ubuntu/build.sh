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

set -e

declare -i EMULATOR_PID=0

function emulator::build_and_test() {
  # Fail on any error.
  set -e

  # Switch to source root so that version extraction and mock work.
  if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
    # Switch to source root.
    cd ${KOKORO_ARTIFACTS_DIR}/git/cloud-spanner-emulator
  fi

  # Display commands being run.
  set -x

  # Skip the build if we are mocking.
  if [[ "${CLOUD_SPANNER_EMULATOR_MOCK_BUILD}" == true ]]; then
    BIN_DIR="${KOKORO_ARTIFACTS_DIR:-/tmp}/bin"
    mkdir -p ${BIN_DIR}
    tar -C "${BIN_DIR}" -czf "${BIN_DIR}/cloud-spanner-emulator-linux_amd64.tar.gz" --files-from /dev/null
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

    tar -C "${BIN_DIR}" -czf "${BIN_DIR}/cloud-spanner-emulator-linux_amd64.tar.gz" gateway_main emulator_main
  fi

  if [[ $exit_code != 0 ]]; then
    exit $exit_code
  fi
}

declare EMULATOR_HOST_PORT="localhost:9010"

function emulator::start() {
  BIN_DIR="${KOKORO_ARTIFACTS_DIR}/bin"
  set -x

  echo "Starting emulator..."
  "${BIN_DIR}"/emulator_main --host_port="${EMULATOR_HOST_PORT}" &
  EMULATOR_PID=$!
}

function emulator::stop() {
  echo "Shutting down emulator..."
  kill -9 "${EMULATOR_PID}"
}

function emulator::run_go_client_library_integration_tests() {
  set -x
  emulator::start

  SPANNER_GO_DIR="${KOKORO_ARTIFACTS_DIR}/go-client/spanner"
  cd "${KOKORO_ARTIFACTS_DIR}"
  if [[ ! -d "${SPANNER_GO_DIR}" ]]; then
    GO_CLIENT_DIR="go-client"
    mkdir "${GO_CLIENT_DIR}"
    cd "${GO_CLIENT_DIR}"
    git init
    git remote add -f origin https://github.com/googleapis/google-cloud-go.git
    git config core.sparseCheckout true
    echo "spanner/" >> .git/info/sparse-checkout
    git pull origin master
  fi

  export SPANNER_EMULATOR_HOST="${EMULATOR_HOST_PORT}"
  export GCLOUD_TESTS_GOLANG_PROJECT_ID=emulator-test-project
  cd "${SPANNER_GO_DIR}"

  # Run the client library integration tests.
  set +e
  go test -v -run '^TestIntegration_'
  exit_code=$?
  set -e

  emulator::stop
  if [[ $exit_code != 0 ]]; then
    echo "Go integration tests failed..."
    exit $exit_code
  fi
}

# By default build the emulator and run unit/conformance tests.
emulator::build_and_test

# For continuous jobs run the client library integration tests.
if [[ "${KOKORO_JOB_NAME}" == "cloud_spanner_emulator/gcp_ubuntu/continuous" ]]; then
  # Run integration tests.
  emulator::run_go_client_library_integration_tests
fi
