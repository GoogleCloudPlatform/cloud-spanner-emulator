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

#include "backend/schema/catalog/index.h"

#include <iterator>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "backend/actions/index.h"
#include "backend/common/indexing.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Returns the row from the indexed table for the given key.
absl::StatusOr<Row> ReadBaseTableRow(
    const ActionContext* ctx, const Table* table, const Key& key,
    const std::vector<const Column*>& base_columns) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(table, KeyRange::Point(key), base_columns,
                         /*allow_pending_commit_timestamps_in_read=*/true));

  Row base_row;
  if (itr->Next()) {
    for (int i = 0; i < itr->NumColumns(); ++i) {
      base_row[base_columns[i]] = itr->ColumnValue(i);
    }
  }
  GOOGLESQL_RETURN_IF_ERROR(itr->Status());
  return base_row;
}

}  // namespace

IndexEffector::IndexEffector(const Index* index) : index_(index) {
  // Save the base table columns corresponding to the index data table.
  for (const Column* column : index->index_data_table()->columns()) {
    base_columns_.emplace_back(column->source_column());
  }
}

absl::Status IndexEffector::Effect(const ActionContext* ctx,
                                   const InsertOp& op) const {
  // Compute the index key and column values.
  Row base_row = MakeRow(op.columns, op.values);
  GOOGLESQL_ASSIGN_OR_RETURN(Key index_key, ComputeIndexKey(base_row, index_));
  ValueList index_values = ComputeIndexValues(base_row, index_);
  if (ShouldFilterIndexKeyOrValue(index_, index_key, base_row)) {
    return absl::OkStatus();
  }

  // Insert the new row in the index.
  ctx->effects()->Insert(index_->index_data_table(), index_key,
                         index_->index_data_table()->columns(), index_values);
  return absl::OkStatus();
}

absl::Status IndexEffector::Effect(const ActionContext* ctx,
                                   const UpdateOp& op) const {
  // Read the current base row values from the indexed table.
  GOOGLESQL_ASSIGN_OR_RETURN(Row base_row,
                   ReadBaseTableRow(ctx, op.table, op.key, base_columns_));
  if (base_row.empty()) {
    return error::Internal(
        absl::StrCat("Missing row from base table when an Update index effect "
                     "is executed. Base Table: ",
                     op.table->Name(), " Key: ", op.key.DebugString()));
  }
  // If a previous index entry existed, delete it.
  GOOGLESQL_ASSIGN_OR_RETURN(Key old_index_key, ComputeIndexKey(base_row, index_));
  if (!ShouldFilterIndexKeyOrValue(index_, old_index_key, base_row)) {
    ctx->effects()->Delete(index_->index_data_table(), old_index_key);
  }

  // Patch new values into value map.
  Row new_row = base_row;
  for (int i = 0; i < op.columns.size(); ++i) {
    new_row[op.columns[i]] = op.values[i];
  }
  for (const KeyColumn* const key_col : index_->key_columns()) {
    const Column* col = key_col->column();
    if (col->source_column() != nullptr) {
      col = col->source_column();
    }
    auto old_it = base_row.find(col);
    auto new_it = new_row.find(col);
    // Index key columns cannot reference a pending commit timestamp.
    if (old_it != base_row.end() && new_it != new_row.end() &&
        old_it->second != new_it->second &&
        ctx->store()->HasPendingCommitTimestamp(col)) {
      return error::CannotReadPendingCommitTimestamp(
          absl::StrCat("Column ", col->Name()));
    }
  }
  GOOGLESQL_ASSIGN_OR_RETURN(Key new_index_key, ComputeIndexKey(new_row, index_));
  ValueList index_values = ComputeIndexValues(new_row, index_);
  if (ShouldFilterIndexKeyOrValue(index_, new_index_key, new_row)) {
    return absl::OkStatus();
  }

  // Insert the new row in the index.
  ctx->effects()->Insert(index_->index_data_table(), new_index_key,
                         index_->index_data_table()->columns(), index_values);
  return absl::OkStatus();
}

absl::Status IndexEffector::Effect(const ActionContext* ctx,
                                   const DeleteOp& op) const {
  // Read base row values.
  GOOGLESQL_ASSIGN_OR_RETURN(Row base_row,
                   ReadBaseTableRow(ctx, op.table, op.key, base_columns_));

  // Did not find an entry to delete from the index.
  if (base_row.empty()) {
    return absl::OkStatus();
  }
  for (const KeyColumn* const key_col : index_->key_columns()) {
    const Column* col = key_col->column();
    if (col->source_column() != nullptr) {
      col = col->source_column();
    }
    if (ctx->store()->HasPendingCommitTimestamp(col)) {
      return error::CannotReadPendingCommitTimestamp(
          absl::StrCat("Column ", col->Name()));
    }
  }

  // Compute the index key to delete.
  GOOGLESQL_ASSIGN_OR_RETURN(Key index_key, ComputeIndexKey(base_row, index_));
  if (ShouldFilterIndexKeyOrValue(index_, index_key, base_row)) {
    return absl::OkStatus();
  }

  // Delete the row from the index.
  ctx->effects()->Delete(index_->index_data_table(), index_key);
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
