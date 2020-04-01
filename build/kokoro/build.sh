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

# Fail on any error
set -e

# This script assumes that the following variables have been set:
# ${PATH}          Contains the directory where gnu-sed can be found.
#                  This is required as ZetaSQL has a build step which depends on
#                  gnu-sed. MacOS contains an older version of sed which will
#                  fail. So PATH needs to resolve sed to gnu-sed. On MacOS, we
#                  install gnu-sed via brew. See kokoro/macos_external/build.sh.
#
# ${GCLOUD_DIR}    Contains the directory where gcloud can be found.
#                  This is required as the gcloud tests need to find the gcloud
#                  binary inside a python script. We don't rely on gcloud being
#                  found on PATH as we run the subprocess from inside python
#                  without shell=True.
#
# ${GO_BINARY_DIR} Contains the directory where bazel places go binaries.
#                  This is required because unlike C++ binaries, bazel places go
#                  binaries inside a platform-specific dir. The gcloud python
#                  test runner uses this variable to find the go gateway binary.
#
# Normally, this script is not invoked directly. Use the platform-specific
# scripts instead, e.g. ./kokoro/gcp_ubuntu/build.sh for linux.
if [[ -z "${GCLOUD_DIR}" ]] || [[ -z "${GO_BINARY_DIR}" ]]; then
  echo "One or more required environment variables have not been set."
  echo "Perhaps you intended to invoke one of the platform specific scripts."
  echo "e.g. ./kokoro/gcp_ubuntu/build.sh"
  exit 1
fi

# Display commands being run.
set -x

# Build all binaries.
time bazel build -c opt ...

# Run all tests.
time bazel test -c opt ...
