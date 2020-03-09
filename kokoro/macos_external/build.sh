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

# Display commands being run.
set -x

# If running under kokoro, setup the environment.
if [[ -n "${KOKORO_ARTIFACTS_DIR}" ]]; then
  # Use the version of bazel that is used in corp.
  use_bazel.sh 0.29.0

  # Install gnu-sed (required by ZetaSQL).
  HOMEBREW_NO_AUTO_UPDATE=1 brew install gnu-sed

  # Switch to source root.
  cd ${KOKORO_ARTIFACTS_DIR}/git/cloud-spanner-emulator
fi

# Ensure that bazel finds gnu-sed before system sed. On a corp laptop, you can
# install this manually with homebrew (brew install gnu-sed).
export PATH=/usr/local/opt/gnu-sed/libexec/gnubin:$PATH

# Directory in which gcloud is located (gcloud should be accessible via PATH for
# this to work).
export GCLOUD_DIR=$(dirname $(which gcloud))

# Directory in which the go compiler places binaries in bazel.
export GO_BINARY_DIR=darwin_amd64_stripped

# Run the build script.
./kokoro/build.sh
