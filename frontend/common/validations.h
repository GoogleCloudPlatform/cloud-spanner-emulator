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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_VALIDATIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_VALIDATIONS_H_

#include <memory>

#include "absl/status/status.h"
#include "frontend/entities/transaction.h"

namespace google::spanner::emulator::frontend {

namespace spanner_api = ::google::spanner::v1;

// Validates the provided directed reads option. For validation requirements,
// see
// https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#google.spanner.v1.DirectedReadOptions
absl::Status ValidateDirectedReadsOption(
    const spanner_api::DirectedReadOptions& directed_read_options,
    std::shared_ptr<Transaction> txn);

}  // namespace google::spanner::emulator::frontend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_VALIDATIONS_H_
