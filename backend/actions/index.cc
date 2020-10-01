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

#include "backend/actions/index.h"

#include <iterator>

#include "zetasql/base/statusor.h"
#include "backend/common/indexing.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Returns the row from the indexed table for the given key.
zetasql_base::StatusOr<Row> ReadBaseTableRow(
    const ActionContext* ctx, const Table* table, const Key& key,
    const std::vector<const Column*>& base_columns) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(table, KeyRange::Point(key), base_columns));

  Row base_row;
  if (itr->Next()) {
    for (int i = 0; i < itr->NumColumns(); ++i) {
      base_row[base_columns[i]] = itr->ColumnValue(i);
    }
  }
  ZETASQL_RETURN_IF_ERROR(itr->Status());
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
  ZETASQL_ASSIGN_OR_RETURN(Key index_key, ComputeIndexKey(base_row, index_));
  ValueList index_values = ComputeIndexValues(base_row, index_);
  if (ShouldFilterIndexKey(index_, index_key)) {
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
  ZETASQL_ASSIGN_OR_RETURN(Row base_row,
                   ReadBaseTableRow(ctx, op.table, op.key, base_columns_));
  if (base_row.empty()) {
    return error::Internal(
        absl::StrCat("Missing row from base table when an Update index effect "
                     "is executed. Base Table: ",
                     op.table->Name(), " Key: ", op.key.DebugString()));
  }

  // If a previous index entry existed, delete it.
  ZETASQL_ASSIGN_OR_RETURN(Key old_index_key, ComputeIndexKey(base_row, index_));
  if (!ShouldFilterIndexKey(index_, old_index_key)) {
    ctx->effects()->Delete(index_->index_data_table(), old_index_key);
  }

  // Patch new values into value map.
  for (int i = 0; i < op.columns.size(); ++i) {
    base_row[op.columns[i]] = op.values[i];
  }
  ZETASQL_ASSIGN_OR_RETURN(Key new_index_key, ComputeIndexKey(base_row, index_));
  ValueList index_values = ComputeIndexValues(base_row, index_);
  if (ShouldFilterIndexKey(index_, new_index_key)) {
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
  ZETASQL_ASSIGN_OR_RETURN(Row base_row,
                   ReadBaseTableRow(ctx, op.table, op.key, base_columns_));

  // Did not find an entry to delete from the index.
  if (base_row.empty()) {
    return absl::OkStatus();
  }

  // Compute the index key to delete.
  ZETASQL_ASSIGN_OR_RETURN(Key index_key, ComputeIndexKey(base_row, index_));
  if (ShouldFilterIndexKey(index_, index_key)) {
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
