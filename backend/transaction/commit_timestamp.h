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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_COMMIT_TIMESTAMP_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_COMMIT_TIMESTAMP_H_

#include "zetasql/public/type.h"
#include "zetasql/base/statusor.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/storage/in_memory_iterator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Helper methods to validate that user provided timestamp value for a commit
// timestamp column is not in future.
absl::Status ValidateCommitTimestampValueNotInFuture(
    const zetasql::Value& value, absl::Time now);

absl::Status ValidateCommitTimestampKeySetForDeleteOp(const Table* table,
                                                      const KeySet& set,
                                                      absl::Time now);

// Helper methods to set commit timestamp sentinel, if user requested to store
// or read commit timestamp atomically in a timestamp column or timestamp key
// column with allow_commit_timestamp set to true.
zetasql_base::StatusOr<ValueList> MaybeSetCommitTimestampSentinel(
    absl::Span<const Column* const> columns, const ValueList& row);

zetasql_base::StatusOr<KeyRange> MaybeSetCommitTimestampSentinel(
    absl::Span<const KeyColumn* const> primary_key, const KeyRange& key_range);

// Returns true if one of the values for a given column contains timestamp
// sentinel value.
bool IsPendingCommitTimestamp(const Column* column,
                              const zetasql::Value& column_value);

// Returns true if given key contains key part with timestamp sentinel value.
bool HasPendingCommitTimestampInKey(const Table* table, const Key& key);

// Replace commit timestamp sentinel value, if present, with transaction
// commit timestamp for the given column value.
zetasql::Value MaybeSetCommitTimestamp(const Column* column,
                                         const zetasql::Value& column_value,
                                         absl::Time commit_timestamp);

// Replace commit timestamp sentinel value, if present, with transaction
// commit timestamp for the given key column values.
Key MaybeSetCommitTimestamp(absl::Span<const KeyColumn* const> primary_key,
                            Key key, absl::Time commit_timestamp);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_COMMIT_TIMESTAMP_H_
