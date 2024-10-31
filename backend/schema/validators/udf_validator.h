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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_UDF_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_UDF_VALIDATOR_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Implementation of UDF::Validate() / UDF::ValidateUpdate().
class UdfValidator {
 public:
  static absl::Status Validate(const Udf* udf,
                               SchemaValidationContext* context);

  static absl::Status ValidateUpdate(const Udf* udf, const Udf* old_udf,
                                     SchemaValidationContext* context);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_UDF_VALIDATOR_H_
