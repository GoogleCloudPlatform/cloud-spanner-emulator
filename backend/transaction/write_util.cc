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
#include "backend/transaction/commit_timestamp.h"
#include "backend/transaction/resolve.h"
#include "backend/transaction/transaction_store.h"
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
