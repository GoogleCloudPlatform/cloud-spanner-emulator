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

#include "backend/transaction/foreign_key_restrictions.h"

#include <set>
#include <string>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/schema.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

bool MutationContainReferencedColumn(
    const std::vector<const Column*>& columns,
    const absl::flat_hash_set<const Column*>& referenced_columns) {
  bool has_referenced_columns = false;
  for (const auto& column : columns) {
    if (referenced_columns.contains(column)) {
      has_referenced_columns = true;
      break;
    }
  }
  return has_referenced_columns;
}

absl::Status ForeignKeyRestrictions::ValidateReferencedDeleteMods(
    const std::string& table_name, std::vector<KeyRange>& key_ranges) {
  std::set<Key>& inserted_keys = inserted_keys_by_referenced_table_[table_name];
  std::vector<KeyRange>& deleted_keys =
      deleted_key_ranges_by_referenced_table_[table_name];
  for (const auto& key_range : key_ranges) {
    for (const Key& key : inserted_keys) {
      if (key_range.Contains(key)) {
        return error::ForeignKeyReferencedRestrictionInTransaction(
            table_name, key_range.DebugString());
      }
    }
    deleted_keys.push_back(key_range);
  }
  return absl::OkStatus();
}

absl::Status ForeignKeyRestrictions::ValidateReferencedMods(
    const std::vector<WriteOp>& write_ops, const std::string& table_name,
    const Schema* schema) {
  if (!referenced_columns_by_referenced_table_.contains(table_name)) {
    const Table* table = schema->FindTable(table_name);
    for (auto fk : table->referencing_foreign_keys()) {
      if (fk->on_delete_action() == ForeignKey::Action::kCascade) {
        for (const auto& column : fk->referenced_columns()) {
          referenced_columns_by_referenced_table_[table_name].insert(column);
        }
      }
    }
  }
  std::set<Key>& inserted_keys = inserted_keys_by_referenced_table_[table_name];
  std::vector<KeyRange>& deleted_keys =
      deleted_key_ranges_by_referenced_table_[table_name];
  for (const auto& write_op : write_ops) {
    if (std::holds_alternative<DeleteOp>(write_op)) {
      const DeleteOp delete_op = std::get<DeleteOp>(write_op);
      if (inserted_keys.find(delete_op.key) != inserted_keys.end()) {
        return error::ForeignKeyReferencedRestrictionInTransaction(
            table_name, delete_op.key.DebugString());
      } else {
        deleted_keys.push_back(KeyRange(EndpointType::kClosed, delete_op.key,
                                        EndpointType::kClosed, delete_op.key));
      }
    } else if (std::holds_alternative<InsertOp>(write_op)) {
      const InsertOp insert_op = std::get<InsertOp>(write_op);
      if (!MutationContainReferencedColumn(
              insert_op.columns,
              referenced_columns_by_referenced_table_[table_name])) {
        continue;
      }
      for (const auto& key_range : deleted_keys) {
        if (key_range.Contains(insert_op.key)) {
          return error::ForeignKeyReferencedRestrictionInTransaction(
              table_name, insert_op.key.DebugString());
        }
      }
      inserted_keys.insert(insert_op.key);
    } else if (std::holds_alternative<UpdateOp>(write_op)) {
      const UpdateOp update_op = std::get<UpdateOp>(write_op);
      if (!MutationContainReferencedColumn(
              update_op.columns,
              referenced_columns_by_referenced_table_[table_name])) {
        continue;
      }
      for (const auto& key_range : deleted_keys) {
        if (key_range.Contains(update_op.key)) {
          return error::ForeignKeyReferencedRestrictionInTransaction(
              table_name, update_op.key.DebugString());
        }
      }
      inserted_keys.insert(update_op.key);
    } else {
      return absl::InvalidArgumentError("Unsupported write op type.");
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
