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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_COLUMN_VALUE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_COLUMN_VALUE_H_

#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ColumnTypeValidator validates if a given mutation contains values which will
// violate constraints for the corresponding column, including column types.
class ColumnValueValidator : public Validator {
 public:
  explicit ColumnValueValidator(
      const absl::flat_hash_set<std::string> placements)
      : placements_(placements) {}
  ColumnValueValidator() = default;

 private:
  absl::Status Validate(const ActionContext* ctx,
                        const InsertOp& op) const override;
  absl::Status Validate(const ActionContext* ctx,
                        const UpdateOp& op) const override;
  absl::Status Validate(const ActionContext* ctx,
                        const DeleteOp& op) const override;
  absl::Status ValidateInsertUpdateOp(
      const Table* table, const std::vector<const Column*>& columns,
      const std::vector<zetasql::Value>& values, Clock* clock) const;
  absl::Status ValidateColumnStringValue(const Table* table,
                                         const Column* column,
                                         const zetasql::Value& value) const;

  absl::Status ValidateColumnValueType(const Table* table,
                                       const Column* const column,
                                       const zetasql::Value& value) const;

  absl::Status ValidateKeyNotNull(const Table* table, const Key& key) const;

  absl::Status ValidateColumnBytesValue(const Table* table,
                                        const Column* column,
                                        const zetasql::Value& value) const;

  absl::Status ValidateColumnArrayValue(const Table* table,
                                        const Column* column,
                                        const zetasql::Value& value) const;

  absl::Status ValidateColumnTimestampValue(const Column* const column,
                                            const zetasql::Value& value,
                                            Clock* clock) const;

  absl::Status ValidateKeySize(const Table* table, const Key& key) const;

  absl::flat_hash_set<std::string> placements_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_COLUMN_VALUE_H_
