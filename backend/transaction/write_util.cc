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

#include "backend/transaction/write_util.h"

#include <memory>
#include <queue>
#include <vector>

#include "zetasql/public/value.h"
#include "zetasql/base/status.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_join.h"
#include "backend/access/write.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "backend/transaction/read_util.h"
#include "backend/transaction/transaction_store.h"
#include "common/constants.h"
#include "common/errors.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Computes the PrimaryKey from the given row using the key indices provided.
void ComputeKey(const ValueList& row,
                absl::Span<const KeyColumn* const> primary_key,
                const std::vector<absl::optional<int>>& key_indices, Key* key) {
  for (int i = 0; i < primary_key.size(); i++) {
    key->AddColumn(
        key_indices[i].has_value()
            ? row[key_indices[i].value()]
            : zetasql::Value::Null(primary_key[i]->column()->GetType()),
        primary_key[i]->is_descending());
  }
}

// Helper methods to set commit timestamp sentinel, if user requested to store
// or read commit timestamp atomically in a timestamp column or timestamp key
// column with allow_commit_timestamp set to true.
zetasql_base::StatusOr<zetasql::Value> MaybeSetCommitTimestampSentinel(
    const Column* column, zetasql::Value column_value) {
  if (column->GetType()->IsTimestamp() && column->allows_commit_timestamp()) {
    if (!column_value.is_null() && column_value.type()->IsString() &&
        column_value.string_value() == kCommitTimestampIdentifier) {
      return zetasql::values::Timestamp(kCommitTimestampValueSentinel);
    } else if (!column_value.is_null() && column_value.type()->IsTimestamp() &&
               column_value.ToTime() == kCommitTimestampValueSentinel) {
      return error::CommitTimestampInFuture(column_value.ToTime());
    }
  }
  return column_value;
}

zetasql_base::StatusOr<ValueList> MaybeSetCommitTimestampSentinel(
    std::vector<const Column*> columns, const ValueList& row) {
  if (row.empty()) return row;
  ValueList ret_val;
  for (int i = 0; i < row.size(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(ret_val.emplace_back(),
                     MaybeSetCommitTimestampSentinel(columns[i], row[i]));
  }
  return ret_val;
}

zetasql_base::StatusOr<Key> MaybeSetCommitTimestampSentinel(
    absl::Span<const KeyColumn* const> primary_key, Key key) {
  for (int i = 0; i < key.NumColumns(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value,
                     MaybeSetCommitTimestampSentinel(primary_key[i]->column(),
                                                     key.ColumnValue(i)));
    key.SetColumnValue(i, value);
  }
  return key;
}

zetasql_base::StatusOr<KeyRange> MaybeSetCommitTimestampSentinel(
    absl::Span<const KeyColumn* const> primary_key, KeyRange key_range) {
  ZETASQL_RET_CHECK(key_range.IsClosedOpen());
  if (key_range.start_key() >= key_range.limit_key()) {
    // Nothing to be done for empty key range.
    return key_range;
  }
  ZETASQL_ASSIGN_OR_RETURN(Key start_key, MaybeSetCommitTimestampSentinel(
                                      primary_key, key_range.start_key()));
  ZETASQL_ASSIGN_OR_RETURN(Key limit_key, MaybeSetCommitTimestampSentinel(
                                      primary_key, key_range.limit_key()));
  return KeyRange(key_range.start_type(), start_key, key_range.limit_type(),
                  limit_key);
}

zetasql_base::Status ValidateCommitTimestampValueNotInFuture(
    const zetasql::Value& value, absl::Time now) {
  if (!value.is_null() && value.type()->IsTimestamp() && value.ToTime() > now) {
    return error::CommitTimestampInFuture(value.ToTime());
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateCommitTimestampKeyForDeleteOp(const Table* table,
                                                   const Key& key,
                                                   absl::Time now) {
  const absl::Span<const KeyColumn* const> primary_key = table->primary_key();
  for (int i = 0; i < key.NumColumns(); ++i) {
    const Column* column = primary_key.at(i)->column();
    if (column->GetType()->IsTimestamp() && column->allows_commit_timestamp()) {
      ZETASQL_RETURN_IF_ERROR(
          ValidateCommitTimestampValueNotInFuture(key.ColumnValue(i), now));
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateCommitTimestampKeySetForDeleteOp(const Table* table,
                                                      const KeySet& set,
                                                      absl::Time now) {
  for (const Key& key : set.keys()) {
    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeyForDeleteOp(table, key, now));
  }

  for (const KeyRange& key_range : set.ranges()) {
    auto closed_open = key_range.ToClosedOpen();
    if (closed_open.start_key() >= closed_open.limit_key()) {
      // No-op empty key ranges are ignored.
      continue;
    }

    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeyForDeleteOp(
        table, key_range.start_key(), now));
    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeyForDeleteOp(
        table, key_range.limit_key(), now));
  }
  return zetasql_base::OkStatus();
}

}  // namespace

zetasql_base::StatusOr<std::vector<absl::optional<int>>> ExtractPrimaryKeyIndices(
    absl::Span<const std::string> columns,
    absl::Span<const KeyColumn* const> primary_key) {
  std::vector<absl::optional<int>> key_indices;
  // Number of columns should be relatively small, so we just iterate over them
  // to find matches.
  std::vector<std::string> missing_columns;
  for (const auto& key_column : primary_key) {
    int i = 0;
    for (; i < columns.size(); ++i) {
      // Column names are case-insensitive in Cloud Spanner.
      if (zetasql_base::CaseEqual(key_column->column()->Name(), columns[i])) {
        key_indices.push_back(i);
        break;
      }
    }
    // Reached end of columns list, so this key_column was not found.
    if (i == columns.size()) {
      if (key_column->column()->is_nullable()) {
        key_indices.push_back(absl::nullopt);
      } else {
        return error::NullValueForNotNullColumn(
            key_column->column()->table()->Name(),
            key_column->column()->Name());
      }
    }
  }

  return key_indices;
}

zetasql_base::Status FlattenDelete(const MutationOp& mutation_op, const Table* table,
                           std::vector<const Column*> columns,
                           const TransactionStore* transaction_store,
                           std::queue<WriteOp>* write_ops_queue,
                           absl::Time now) {
  // Invalid commit timestamp key ranges for delete ops need to be caught
  // before mutation ops are passed to action manager framework since
  // CanonicalizeKeySetForTable filters out invalid key ranges with future
  // timestamp(s).
  ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeySetForDeleteOp(
      table, mutation_op.key_set, now));

  std::vector<KeyRange> key_ranges;
  CanonicalizeKeySetForTable(mutation_op.key_set, table, &key_ranges);
  for (KeyRange key_range : key_ranges) {
    // Transaction store may contain commit timestamp sentinel value until
    // flush, if requested so in a previous mutation. Hence, convert key
    // values to timestamp sentinel value for reads.
    ZETASQL_ASSIGN_OR_RETURN(key_range, MaybeSetCommitTimestampSentinel(
                                    table->primary_key(), key_range));

    std::unique_ptr<StorageIterator> itr;
    ZETASQL_RETURN_IF_ERROR(transaction_store->Read(table, key_range, columns, &itr));
    while (itr->Next()) {
      write_ops_queue->push(DeleteOp{table, itr->Key()});
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status FlattenMutationOpRow(
    const Table* table, const std::vector<const Column*>& columns,
    const std::vector<absl::optional<int>>& key_indices, ValueList row,
    const MutationOpType& type, const TransactionStore* transaction_store,
    std::queue<WriteOp>* write_ops_queue, absl::Time now) {
  ZETASQL_ASSIGN_OR_RETURN(row, MaybeSetCommitTimestampSentinel(columns, row));
  Key key;
  ComputeKey(row, table->primary_key(), key_indices, &key);
  switch (type) {
    case MutationOpType::kInsert: {
      write_ops_queue->push(InsertOp{table, key, columns, row});
      break;
    }
    case MutationOpType::kUpdate: {
      write_ops_queue->push(UpdateOp{table, key, columns, row});
      break;
    }
    case MutationOpType::kInsertOrUpdate: {
      zetasql_base::Status s = transaction_store->Lookup(table, key,
                                                 /*columns= */ {},
                                                 /*values= */ nullptr);
      if (s.ok()) {
        // Row exists and therefore we should only update.
        write_ops_queue->push(UpdateOp{table, key, columns, row});
      } else if (s.code() == zetasql_base::StatusCode::kNotFound) {
        write_ops_queue->push(InsertOp{table, key, columns, row});
      } else {
        return s;
      }
      break;
    }
    case MutationOpType::kReplace: {
      write_ops_queue->push(DeleteOp{table, key});
      write_ops_queue->push(InsertOp{table, key, columns, row});
      break;
    }
    case MutationOpType::kDelete: {
      break;
    }
  }

  return zetasql_base::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
