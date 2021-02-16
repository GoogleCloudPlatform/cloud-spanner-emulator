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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_MUTATIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_MUTATIONS_H_

#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "absl/status/status.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/schema.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Converts the Mutation proto into a backend mutation using the given schema.
absl::Status MutationFromProto(
    const backend::Schema& schema,
    const google::protobuf::RepeatedPtrField<google::spanner::v1::Mutation>& mutation_pbs,
    backend::Mutation* mutation);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_ACCESS_PROTOS_H_
