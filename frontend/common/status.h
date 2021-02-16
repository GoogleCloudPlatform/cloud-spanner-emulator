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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_STATUS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_STATUS_H_

#include "grpcpp/support/status.h"
#include "absl/status/status.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {

// Converts a absl::Status to a grpc::Status, accounting for max error length.
// If a payload is attached it will become part of the serialized error_details
// within the grpc::Status.
grpc::Status ToGRPCStatus(const absl::Status& status);

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_STATUS_H_
