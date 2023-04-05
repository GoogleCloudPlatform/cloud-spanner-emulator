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
  # Build with optional caching
  if [[ -z "${REMOTE_CACHE}" ]]; then
     bazel build -c opt ...
  else
    bazel build -c opt ... \
    --remote_cache=${REMOTE_CACHE} \
    --google_default_credentials
  fi
  exit_code=$?
  if [[ $exit_code != 0 ]]; then
    set -e
    emulator::copy_logs
    exit $exit_code
  fi

# Run tests without cache
  bazel test -c opt ...
  exit_code=$?
  set -e
  if [[ $exit_code != 0 ]]; then
    emulator::copy_logs
    exit $exit_code
  fi
}

# By default build the emulator and run unit/conformance tests.
emulator::build_and_test

# Copy logs to kokoro artifacts directory
emulator::copy_logs

exit 0
