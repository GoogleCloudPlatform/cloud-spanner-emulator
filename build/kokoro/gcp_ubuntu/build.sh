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

EMULATOR_HOST_PORT="localhost:9010"
EMULATOR_PID=""

function emulator::start() {
  ${EMULATOR_SRC_DIR}/bazel-bin/binaries/emulator_main --host_port="${EMULATOR_HOST_PORT}" &
  EMULATOR_PID=$!
}

function emulator::stop() {
  kill -9 "${EMULATOR_PID}"
}

function emulator::copy_logs() {
  # Check if we are running through Kokoro.
  if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
    find -L . -name "test.log" -exec rename 's/test.log/sponge_log.log/' {} \;
    find -L . -name "sponge_log.log" -exec cp --parents {} "/logs" \;
    find -L . -name "test.xml" -exec rename 's/test.xml/sponge_log.xml/' {} \;
    find -L . -name "sponge_log.xml" -exec cp --parents {} "/logs" \;
  fi
}

function emulator::build_and_test() {
  set +e
  if [[ -z "${REMOTE_CACHE}" ]]; then
     bazel test -c opt ...
  else
    bazel test -c opt ... \
    --remote_cache=${REMOTE_CACHE} \
    --google_default_credentials
  fi
  exit_code=$?
  set -e

  emulator::copy_logs
  if [[ $exit_code != 0 ]]; then
    exit $exit_code
  fi
}

function emulator::run_integration_tests() {
  local client=$1
  export SPANNER_EMULATOR_HOST="${EMULATOR_HOST_PORT}"
  emulator::start

  # Run the client library integration tests.
  cd "${CLIENT_LIB_DIR}"
  echo "Running ${client} integration tests..."
  set +e
  if [[ $client == "go" ]]; then
    cd "go/spanner"
    export GCLOUD_TESTS_GOLANG_PROJECT_ID=emulator-test-project
    go test -v -run '^TestIntegration_'
  elif [[ $client == "java" ]]; then
    cd "java/google-cloud-spanner"
    mvn clean test-compile failsafe:integration-test -DskipITs=false \
    -Dspanner.testenv.instance=""
  elif [[ $client == "cpp" ]]; then
    cd "cpp"
    bazel test --test_env=SPANNER_EMULATOR_HOST=localhost:9010 \
      --test_env=GOOGLE_CLOUD_PROJECT=test-project \
      --test_env=GOOGLE_CLOUD_CPP_SPANNER_INSTANCE=test-instance-a \
      --test_env=GOOGLE_CLOUD_CPP_AUTO_RUN_EXAMPLES=yes \
      --test_env=RUN_SLOW_INTEGRATION_TESTS=yes \
      --nocache_test_results --test_tag_filters=integration-tests ...
  else
    echo "Unrecognized client: \"${client}\"."
  fi
  exit_code=$?
  set -e

  emulator::stop
  if [[ $exit_code != 0 ]]; then
    echo "${client} integration tests failed..."
    exit $exit_code
  fi
}

emulator::continuous_integration() {
  IFS=','
  for client in $CLIENT_INTEGRATION_TESTS
  do
    emulator::run_integration_tests $client
  done
}

# By default build the emulator and run unit/conformance tests.
emulator::build_and_test

# For continuous jobs run the client library integration tests.
emulator::continuous_integration
