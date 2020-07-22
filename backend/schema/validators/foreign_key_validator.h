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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_FOREIGN_KEY_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_FOREIGN_KEY_VALIDATOR_H_

#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Implementation of ForeignKey::Validate(). Foreign keys cannot be updated.
class ForeignKeyValidator {
 public:
  static absl::Status Validate(const ForeignKey* foreign_key,
                               SchemaValidationContext* context);

  static absl::Status ValidateUpdate(const ForeignKey* foreign_key,
                                     const ForeignKey* old_foreign_key,
                                     SchemaValidationContext* context);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_FOREIGN_KEY_VALIDATOR_H_
