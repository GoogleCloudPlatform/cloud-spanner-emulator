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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_VERIFIERS_COLUMN_NULLNESS_VERIFIER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_VERIFIERS_COLUMN_NULLNESS_VERIFIER_H_

#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Verifies that all the entries within a column are not NULL. Needed when
// columns are updated with a 'NOT NULL' property.
absl::Status VerifyColumnNotNull(const Table* table, const Column* column,
                                 const SchemaValidationContext* context);

// Verifies that all of the entries within a column are <= new_max_length.
// This property must be true for resizing a column within a schema update.
absl::Status VerifyColumnLength(const Table* table, const Column* column,
                                int64_t new_max_length,
                                const SchemaValidationContext* context);

// Verifies that a type change of a column is valid. Only STRING to BYTES and
// BYTES to STRING conversions are allowed. Strings are UTF8 encoded, so a
// conversion from BYTES to STRING must have valid UTF8 characters.
absl::Status VerifyColumnTypeChange(const Table* table,
                                    const Column* old_column,
                                    const Column* new_column,
                                    const SchemaValidationContext* context);

// Verifies that all of the entries within a TIMESTAMP column where
// allow_commit_timestamp=true are not in the future. Commit timestamps must be
// before the schema pending commit timestamp supplied by the context.
absl::Status VerifyColumnCommitTimestamp(
    const Table* table, const Column* column,
    const SchemaValidationContext* context);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_VERIFIERS_COLUMN_NULLNESS_VERIFIER_H_
