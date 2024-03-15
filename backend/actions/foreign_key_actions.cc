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

#include "backend/actions/foreign_key_actions.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/common/case.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

bool AreColumnsOrderedPrefixOfPK(
    const absl::Span<const Column* const> columns,
    const absl::Span<const KeyColumn* const> primary_key) {
  if (columns.size() > primary_key.size()) {
    return false;
  }
  for (int i = 0; i < columns.size(); ++i) {
    if (columns.at(i) != primary_key.at(i)->column()) {
      return false;
    }
  }
  return true;
}

bool AreColumnsUnorderedPrefixOfPK(
    const absl::Span<const Column* const> columns,
    const absl::Span<const KeyColumn* const> primary_key) {
  CaseInsensitiveStringSet col_set;
  for (const Column* column : columns) {
    col_set.emplace(column->Name());
  }
  for (int i = 0; i < columns.size(); ++i) {
    const KeyColumn* key_column = primary_key[i];
    if (!col_set.contains(key_column->column()->Name())) {
      // There is a non-PK column. It cannot be a prefix.
      return false;
    }
  }
  // Columns are a prefix of the PK, but are out of order.
  return true;
}

FKPrefixShape GetKeyPrefixShape(
    const absl::Span<const Column* const> columns,
    const absl::Span<const KeyColumn* const> key_columns) {
  if (columns.size() > key_columns.size()) {
    // Too many columns, it cannot be a prefix of the PK.
    return FKPrefixShape::kNone;
  }
  if (AreColumnsOrderedPrefixOfPK(columns, key_columns)) {
    // Columns are a prefix of the PK, and are in order.
    if (columns.size() < key_columns.size()) {
      return FKPrefixShape::kInOrderPrefix;
    }
    // Columns are in the same order and size as the primary key.
    return FKPrefixShape::kInOrder;
  }
  return AreColumnsUnorderedPrefixOfPK(columns, key_columns)
             ? FKPrefixShape::kOutOfOrder
             : FKPrefixShape::kNone;
}

// Returns a vector of the same length as `key_columns` such that the element at
// index i is the index of the element in `columns` that is equal to
// `key_columns[i]`, or `nullopt` if `columns` does not contain
// `key_columns[i]`.
std::vector<std::optional<int>> GetPrimaryKeyColumnPositions(
    const absl::Span<const Column* const> columns,
    const absl::Span<const KeyColumn* const> key_columns) {
  std::vector<std::optional<int>> column_order;
  column_order.reserve(key_columns.size());
  CaseInsensitiveStringMap<int> column_positions;
  for (int i = 0; i < columns.size(); i++) {
    column_positions[columns[i]->Name()] = i;
  }
  for (const KeyColumn* key_column : key_columns) {
    auto it = column_positions.find(key_column->column()->Name());
    if (it != column_positions.end()) {
      column_order.push_back(it->second);
    } else {
      column_order.push_back(std::nullopt);
    }
  }
  return column_order;
}

// Arrange the values in the order of the primary key columns, and return the
// key. If any of the primary key columns are missing from the given column
// order then return the prefix of the primary key.
Key ComputePk(const std::vector<zetasql::Value>& values,
              const std::vector<std::optional<int>>& col_order) {
  std::vector<zetasql::Value> key_value;
  key_value.reserve(col_order.size());
  for (int i = 0; i < col_order.size(); ++i) {
    if (!col_order[i].has_value()) {
      break;
    }
    key_value.push_back(values[col_order[i].value()]);
  }
  return Key(key_value);
}

}  // namespace

ForeignKeyActionEffector::ForeignKeyActionEffector(
    const ForeignKey* foreign_key)
    : foreign_key_(foreign_key),
      on_delete_action_(foreign_key->on_delete_action()),
      referenced_key_prefix_shape_(
          GetKeyPrefixShape(foreign_key->referenced_columns(),
                            foreign_key->referenced_table()->primary_key())),
      referencing_key_prefix_shape_(
          GetKeyPrefixShape(foreign_key->referencing_columns(),
                            foreign_key->referencing_table()->primary_key())) {}

absl::Status ForeignKeyActionEffector::ProcessDeleteForUnorderedReferencingKey(
    const ActionContext* ctx, const Key& referenced_key) const {
  const std::vector<std::optional<int>> column_order =
      GetPrimaryKeyColumnPositions(
          foreign_key_->referencing_columns(),
          foreign_key_->referencing_table()->primary_key());
  // Sort referenced_key by primary key columns of the referencing table.
  Key referencing_key = ComputePk(referenced_key.column_values(), column_order);
  // If the foreign key is a prefix but the order of the columns is
  // different, then we need to read all the rows in the referencing table
  // that match the prefix of the key.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<StorageIterator> itr,
                     ctx->store()->Read(foreign_key_->referencing_table(),
                                        KeyRange::Prefix(referencing_key), {}));
    while (itr->Next()) {
      ctx->effects()->Delete(foreign_key_->referencing_table(), itr->Key());
    }
  return absl::OkStatus();
}

absl::Status ForeignKeyActionEffector::ProcessDeleteForNonPKReferencingKey(
    const ActionContext* ctx, const Key& referenced_key) const {
  // Sort referenced_key by primary key columns of the referencing index
  // table.
  const std::vector<std::optional<int>> column_order =
      GetPrimaryKeyColumnPositions(
          foreign_key_->referencing_columns(),
          foreign_key_->referencing_data_table()->primary_key());
  Key index_key = ComputePk(referenced_key.column_values(), column_order);
  // Retrieve the referencing table key from the row in the referencing
  // index data table that matches the referenced key.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(foreign_key_->referencing_data_table(),
                         KeyRange::Prefix(index_key),
                         foreign_key_->referencing_data_table()->columns()));
  std::vector<const Column*> referenced_columns;
  std::transform(foreign_key_->referencing_data_table()->primary_key().begin(),
                 foreign_key_->referencing_data_table()->primary_key().end(),
                 std::back_inserter(referenced_columns),
                 [](auto key) { return key->column(); });
  std::vector<std::optional<int>> index_column_order =
      GetPrimaryKeyColumnPositions(
          referenced_columns, foreign_key_->referencing_table()->primary_key());
  while (itr->Next()) {
    Key referencing_key =
        ComputePk(itr->Key().column_values(), index_column_order);
    ctx->effects()->Delete(foreign_key_->referencing_table(), referencing_key);
  }
  return absl::OkStatus();
}

absl::Status ForeignKeyActionEffector::ProcessDeleteByKey(
    const ActionContext* ctx, const Key& referenced_key) const {
  switch (referencing_key_prefix_shape_) {
    case FKPrefixShape::kInOrder: {
      // Since the referenced key and primary key have the same shape, we can
      // use the primary key.
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<StorageIterator> itr,
                       ctx->store()->Read(foreign_key_->referencing_table(),
                                          KeyRange::Point(referenced_key), {}));
      while (itr->Next()) {
        ctx->effects()->Delete(foreign_key_->referencing_table(), itr->Key());
      }
      break;
    }
    case FKPrefixShape::kInOrderPrefix: {
      // The referencing table could have multiple rows that reference the same
      // foreign key value.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<StorageIterator> itr,
          ctx->store()->Read(foreign_key_->referencing_table(),
                             KeyRange::Prefix(referenced_key), {}));
      while (itr->Next()) {
        ctx->effects()->Delete(foreign_key_->referencing_table(), itr->Key());
      }
      break;
    }
    case FKPrefixShape::kOutOfOrder:
      ZETASQL_RETURN_IF_ERROR(
          ProcessDeleteForUnorderedReferencingKey(ctx, referenced_key));
      break;
    case FKPrefixShape::kNone:
      ZETASQL_RETURN_IF_ERROR(ProcessDeleteForNonPKReferencingKey(ctx, referenced_key));
      break;
  }
  return absl::OkStatus();
}

absl::Status ForeignKeyActionEffector::EffectForUnorderedReferencedKey(
    const ActionContext* ctx, const DeleteOp& op) const {
  std::vector<std::optional<int>> column_order = GetPrimaryKeyColumnPositions(
      foreign_key_->referenced_columns(),
      foreign_key_->referenced_table()->primary_key());
  Key referenced_key = ComputePk(op.key.column_values(), column_order);
  ZETASQL_RETURN_IF_ERROR(ProcessDeleteByKey(ctx, referenced_key));
  return absl::OkStatus();
}

absl::Status ForeignKeyActionEffector::EffectForNonPKReferencedKey(
    const ActionContext* ctx, const DeleteOp& op) const {
  // Read the values of all non-key columns in the referenced table that is
  // referenced by a foreign key.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<StorageIterator> itr,
                   ctx->store()->Read(foreign_key_->referenced_table(),
                                      KeyRange::Prefix(op.key),
                                      foreign_key_->referenced_columns()));
  while (itr->Next()) {
    Key referenced_key;
    for (int i = 0; i < itr->NumColumns(); ++i) {
      referenced_key.AddColumn(itr->ColumnValue(i));
    }
    ZETASQL_RETURN_IF_ERROR(ProcessDeleteByKey(ctx, referenced_key));
  }
  return absl::OkStatus();
}

absl::Status ForeignKeyActionEffector::Effect(const ActionContext* ctx,
                                              const DeleteOp& op) const {
  switch (referenced_key_prefix_shape_) {
    case FKPrefixShape::kInOrder:
      ZETASQL_RETURN_IF_ERROR(ProcessDeleteByKey(ctx, op.key));
      break;
    case FKPrefixShape::kInOrderPrefix:
      // Need to supply key prefix, because the prefix alone could match
      // multiple rows in the referencing table.
      ZETASQL_RETURN_IF_ERROR(ProcessDeleteByKey(
          ctx, op.key.Prefix(foreign_key_->referenced_columns().size())));
      break;
    case FKPrefixShape::kOutOfOrder:
      ZETASQL_RETURN_IF_ERROR(EffectForUnorderedReferencedKey(ctx, op));
      break;
    case FKPrefixShape::kNone:
      ZETASQL_RETURN_IF_ERROR(EffectForNonPKReferencedKey(ctx, op));
      break;
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
