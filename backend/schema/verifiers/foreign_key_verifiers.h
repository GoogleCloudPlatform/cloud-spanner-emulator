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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VERIFIERS_FOREIGN_KEY_VERIFIERS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VERIFIERS_FOREIGN_KEY_VERIFIERS_H_

#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Verifies that existing rows in the referencing table has corresponding rows
// in the referenced table.
absl::Status VerifyForeignKeyData(const ForeignKey* foreign_key,
                                  const SchemaValidationContext* context);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VERIFIERS_FOREIGN_KEY_VERIFIERS_H_
