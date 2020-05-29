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

#include "backend/transaction/flush.h"

#include "backend/common/variant.h"
#include "backend/transaction/commit_timestamp.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status FlushInsert(const InsertOp& insert_op, Storage* base_storage,
                         absl::Time commit_timestamp) {
  const Table* table = insert_op.table;
  const Key key = MaybeSetCommitTimestamp(table->primary_key(), insert_op.key,
                                          commit_timestamp);
  std::vector<ColumnID> column_ids;
  ValueList column_values;
  for (int i = 0; i < insert_op.columns.size(); i++) {
    column_ids.push_back(insert_op.columns[i]->id());
    column_values.push_back(MaybeSetCommitTimestamp(
        insert_op.columns[i], insert_op.values[i], commit_timestamp));
  }
  return base_storage->Write(commit_timestamp, table->id(), key, column_ids,
                             column_values);
}

absl::Status FlushUpdate(const UpdateOp& update_op, Storage* base_storage,
                         absl::Time commit_timestamp) {
  const Table* table = update_op.table;
  const Key key = MaybeSetCommitTimestamp(table->primary_key(), update_op.key,
                                          commit_timestamp);
  std::vector<ColumnID> column_ids;
  ValueList column_values;
  for (int i = 0; i < update_op.columns.size(); i++) {
    column_ids.push_back(update_op.columns[i]->id());
    column_values.push_back(MaybeSetCommitTimestamp(
        update_op.columns[i], update_op.values[i], commit_timestamp));
  }
  return base_storage->Write(commit_timestamp, table->id(), key, column_ids,
                             column_values);
}

absl::Status FlushDelete(const DeleteOp& delete_op, Storage* base_storage,
                         absl::Time commit_timestamp) {
  return base_storage->Delete(commit_timestamp, delete_op.table->id(),
                              KeyRange::Point(delete_op.key));
}

}  // namespace

absl::Status FlushWriteOpsToStorage(const std::vector<WriteOp>& write_ops,
                                    Storage* base_storage,
                                    absl::Time commit_timestamp) {
  for (const auto& write_op : write_ops) {
    ZETASQL_RETURN_IF_ERROR(std::visit(
        overloaded{
            [&](const InsertOp& insert_op) {
              return FlushInsert(insert_op, base_storage, commit_timestamp);
            },
            [&](const UpdateOp& update_op) {
              return FlushUpdate(update_op, base_storage, commit_timestamp);
            },
            [&](const DeleteOp& delete_op) {
              return FlushDelete(delete_op, base_storage, commit_timestamp);
            },
        },
        write_op));
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
