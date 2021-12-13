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

EMULATOR_PID=""

function emulator::start() {
  ${EMULATOR_SRC_DIR}/bazel-bin/binaries/${GO_BINARY_DIR}/gateway_main &
  EMULATOR_PID=$!
}

function emulator::stop() {
  pkill -9 -P "${EMULATOR_PID}"
}

function emulator::copy_logs() {
  if [[ -n "${COPY_LOGS_TO}" ]]; then
    cd /root
    find . -iname "test.log" -exec rename 's/test.log/sponge_log.log/' '{}' \;
    find . -iname "sponge_log.log" -exec cp --parents {} "$COPY_LOGS_TO" \;
    find . -iname "test.xml" -exec rename 's/test.xml/sponge_log.xml/' '{}' \;
    find . -iname "sponge_log.xml" -exec cp --parents {} "$COPY_LOGS_TO" \;
    # logs directory doesn't always have correct permissions for Kokoro to be
    # able to copy the logs, so we set it to 755.
    chmod -R 755 "$COPY_LOGS_TO"
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

  if [[ $exit_code != 0 ]]; then
    emulator::copy_logs
    exit $exit_code
  fi
}

function emulator::run_integration_tests() {
  local client=$1
  export SPANNER_EMULATOR_HOST="localhost:9010"
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
    cd "cpp/google/cloud/spanner/"
    bazel test --test_env=SPANNER_EMULATOR_HOST=localhost:9010 \
      --test_env=GOOGLE_CLOUD_PROJECT=test-project \
      --test_env=GOOGLE_CLOUD_CPP_SPANNER_TEST_INSTANCE_ID=test-instance-a \
      --test_env=GOOGLE_CLOUD_CPP_AUTO_RUN_EXAMPLES=yes \
      --test_env=GOOGLE_CLOUD_CPP_SPANNER_SLOW_INTEGRATION_TESTS=yes \
      --nocache_test_results --test_tag_filters=integration-test ...
  elif [[ $client == "py" ]]; then
    cd "py"
    export GCLOUD_PROJECT=emulator-test-project
    export GOOGLE_CLOUD_TESTS_CREATE_SPANNER_INSTANCE=true
    nox -s system
  elif [[ $client == "nodejs" ]]; then
    cd "nodejs"
    export GCLOUD_PROJECT=emulator-project
    export SPANNER_EMULATOR_HOST=localhost:9010
    npm install
    npm run system-test
  elif [[ $client == "php" ]]; then
    cd "php"
    export GCLOUD_PROJECT=emulator-project
    export SPANNER_EMULATOR_HOST=localhost:9010
    gcloud spanner instances create google-cloud-php-system-tests \
      --description emulator-instance --nodes 1 --config test-config \
      --project emulator-project
    composer install
    vendor/bin/phpunit -c phpunit-system.xml.dist --group=spanner
  elif [[ $client == "ruby" ]]; then
    cd "ruby/google-cloud-spanner"
    export SPANNER_EMULATOR_HOST=localhost:9010
    export SPANNER_TEST_PROJECT=emulator-project
    eval "$(rbenv init - bash)"
    gem install bundle
    gem install bundler
    bundle config set --local path 'vendor/bundle'
    bundle install
    bundle exec rake acceptance
  elif [[ $client == "csharp" ]]; then
    cd "c#/apis/Google.Cloud.Spanner.Data/Google.Cloud.Spanner.Data.IntegrationTests"
    gcloud spanner instances create spannerintegration \
      --config=emulator-config --description="Test Instance" --nodes=1
    export TEST_PROJECT=emulator-project
    export SPANNER_EMULATOR_HOST=localhost:9010
    dotnet test
  else
    echo "Unrecognized client: \"${client}\"."
  fi
  exit_code=$?
  set -e

  emulator::stop
  if [[ $exit_code != 0 ]]; then
    echo "${client} integration tests failed..."
    emulator::copy_logs
    exit $exit_code
  fi
}

emulator::continuous_integration() {
  # Create a temporary config to avoid overwriting the existing active gcloud
  # configuration state because the gcloud directory is shared.
  GCLOUD_CONFIG_TO_RESTORE=$(gcloud config configurations list \
                               --filter='IS_ACTIVE:true' --format "value(NAME)")
  # Give temp config a random name to avoid conflicting with existing gcloud
  # configs.
  GCLOUD_TEMP_CONFIG="emulator-build-temp-config-$RANDOM"
  gcloud config configurations create $GCLOUD_TEMP_CONFIG
  gcloud config set auth/disable_credentials true
  gcloud config set project emulator-project
  gcloud config set api_endpoint_overrides/spanner http://localhost:9020/

  IFS=','
  for client in $CLIENT_INTEGRATION_TESTS
  do
    emulator::run_integration_tests $client
  done

  # Restore gcloud configuration.
  gcloud config configurations activate $GCLOUD_CONFIG_TO_RESTORE
  gcloud config configurations delete $GCLOUD_TEMP_CONFIG --quiet
}

# By default build the emulator and run unit/conformance tests.
emulator::build_and_test

# For continuous jobs run the client library integration tests.
emulator::continuous_integration

# Copy logs to kokoro artifacts directory
emulator::copy_logs

exit 0
