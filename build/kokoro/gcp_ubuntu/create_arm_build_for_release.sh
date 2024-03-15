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
# Run this in an arm machine after making a release in github
sudo apt-get update
sudo apt-get install -qq -y jq curl
cd /tmp
rm -rf cloud-spanner-emulator
git clone https://github.com/GoogleCloudPlatform/cloud-spanner-emulator.git
cd cloud-spanner-emulator
docker rmi gcr.io/cloud-spanner-emulator/emulator-arm64 -f
sudo docker buildx build . -t gcr.io/cloud-spanner-emulator/emulator-arm64 -f build/docker/Dockerfile.ubuntu  --platform=linux/arm64 --push
container_id=$(sudo docker create gcr.io/cloud-spanner-emulator/emulator-arm64)
sudo docker cp "$container_id":gateway_main .
sudo docker cp "$container_id":emulator_main .
VERSION=$(curl https://api.github.com/repos/GoogleCloudPlatform/cloud-spanner-emulator/releases/latest | jq '.name'[1:])
tar -cvf cloud-spanner-emulator_linux_arm64-${VERSION}.tar.gz emulator_main gateway_main
gsutil cp cloud-spanner-emulator_linux_arm64-${VERSION}.tar.gz gs://cloud-spanner-emulator/releases/${VERSION}
