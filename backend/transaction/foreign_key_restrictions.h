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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_FOREIGN_KEY_RESTRICTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_FOREIGN_KEY_RESTRICTIONS_H_

#include <set>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/actions/ops.h"
#include "backend/common/case.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class ForeignKeyRestrictions {
 public:
  ForeignKeyRestrictions() = default;

  // Validates that the constructive operation on the referenced tables do not
  // conflict with any delete mutation within a transaction.
  // Note: Replace mutation is converted into Delete+Insert mutation.
  absl::Status ValidateReferencedMods(const std::vector<WriteOp>& write_ops,
                                      const std::string& table_name,
                                      const Schema* schema);

  // Validates that the delete mutation on the referenced tables do not conflict
  // with any constructive mutation within a transaction.
  absl::Status ValidateReferencedDeleteMods(const std::string& table_name,
                                            std::vector<KeyRange>& key_ranges);

 private:
  // Within the transaction, track writes and deletes on the tables with
  // foreign key delete cascade action.
  CaseInsensitiveStringMap<std::vector<KeyRange>>
      deleted_key_ranges_by_referenced_table_;
  CaseInsensitiveStringMap<std::set<Key>> inserted_keys_by_referenced_table_;
  CaseInsensitiveStringMap<absl::flat_hash_set<const Column*>>
      referenced_columns_by_referenced_table_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_FOREIGN_KEY_RESTRICTIONS_H_
