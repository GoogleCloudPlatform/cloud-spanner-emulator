//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_LABELS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_LABELS_H_

#include <string>

#include "google/protobuf/map.h"
#include "absl/status/status.h"
#include "re2/re2.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Labels are free-form key-value pairs that can be attached to a resource.
//
// In Cloud Spanner, labels can be attached to instances and sessions. The
// emulator will validate the provided labels but does not support filtering
// using labels in the ListInstances or ListSessions apis.
//
// For more information about labels and their usage, see https://goo.gl/xmQnxf.
using Labels = std::map<std::string, std::string>;

// Validates the provided labels. For validation requirements, see
//    https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#session
absl::Status ValidateLabels(
    const google::protobuf::Map<std::string, std::string>& labels);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_LABELS_H_
