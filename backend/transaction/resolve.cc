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

#include "backend/transaction/resolve.h"

#include <vector>

#include "zetasql/base/statusor.h"
#include "absl/strings/match.h"
#include "backend/common/case.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/column.h"
#include "backend/transaction/commit_timestamp.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

KeySet SetSortOrder(const Table* table, const KeySet& key_set) {
  KeySet result_set;
  for (auto& key : key_set.keys()) {
    Key new_key(key);
    // Set ascending and descending values for the keys.
    for (int i = 0; i < new_key.NumColumns(); ++i) {
      new_key.SetColumnDescending(i, table->primary_key()[i]->is_descending());
    }
    result_set.AddKey(new_key);
  }

  for (auto& range : key_set.ranges()) {
    KeyRange new_range(range);
    // Set ascending and descending values for the keys.
    for (int i = 0; i < range.start_key().NumColumns(); ++i) {
      new_range.start_key().SetColumnDescending(
          i, table->primary_key()[i]->is_descending());
    }
    for (int i = 0; i < range.limit_key().NumColumns(); ++i) {
      new_range.limit_key().SetColumnDescending(
          i, table->primary_key()[i]->is_descending());
    }
    result_set.AddRange(new_range);
  }
  return result_set;
}

void CanonicalizeKeySetForTable(const KeySet& set, const Table* table,
                                std::vector<KeyRange>* ranges) {
  KeySet ordered_set = SetSortOrder(table, set);
  MakeDisjointKeyRanges(ordered_set, ranges);
}

absl::Status ValidateColumnsAreNotDuplicate(
    const std::vector<std::string>& column_names) {
  CaseInsensitiveStringSet columns;
  for (const std::string& column_name : column_names) {
    if (columns.find(column_name) != columns.end()) {
      return error::MultipleValuesForColumn(column_name);
    }
    columns.insert(column_name);
  }
  return absl::OkStatus();
}

absl::Status ValidateNotNullColumnsPresent(
    const Table* table, const std::vector<const Column*>& columns) {
  // TODO: Find a way of doing this without creating a hash set for
  // every insert.
  absl::flat_hash_set<const Column*> inserted_columns;
  for (const auto column : columns) {
    if (!column->is_nullable()) {
      inserted_columns.insert(column);
    }
  }
  for (const auto column : table->columns()) {
    // Writing null to non-nullable generated columns is temporarily fine,
    // since the violation may be fixed later by a generated operation to update
    // the column.
    if (!column->is_nullable() && !column->is_generated() &&
        !inserted_columns.contains(column)) {
      return error::NonNullValueNotSpecifiedForInsert(table->Name(),
                                                      column->Name());
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateGeneratedColumnsNotPresent(
    const Table* table, const std::vector<const Column*>& columns) {
  for (const Column* column : columns) {
    if (column->is_generated() &&
        table->FindKeyColumn(column->Name()) == nullptr) {
      return error::CannotWriteToGeneratedColumn(table->Name(), column->Name());
    }
  }
  return absl::OkStatus();
}

// Extracts the primary key column indices from the given list of columns. The
// returned indices will be in the order specified by the primary key. Nullable
// primary key columns do not need to be specified, in which the index entry
// will be nullopt.
zetasql_base::StatusOr<std::vector<absl::optional<int>>> ExtractPrimaryKeyIndices(
    absl::Span<const Column* const> columns,
    absl::Span<const KeyColumn* const> primary_key) {
  std::vector<absl::optional<int>> key_indices;
  // Number of columns should be relatively small, so we just iterate over them
  // to find matches.
  std::vector<std::string> missing_columns;
  for (const auto& key_column : primary_key) {
    int i = 0;
    for (; i < columns.size(); ++i) {
      // Column names are case-insensitive in Cloud Spanner.
      if (absl::EqualsIgnoreCase(key_column->column()->Name(),
                                 columns[i]->Name())) {
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

// Computes the PrimaryKey from the given row using the key indices provided.
Key ComputeKey(const ValueList& row,
               absl::Span<const KeyColumn* const> primary_key,
               const std::vector<absl::optional<int>>& key_indices) {
  Key key;
  for (int i = 0; i < primary_key.size(); i++) {
    key.AddColumn(
        key_indices[i].has_value()
            ? row[key_indices[i].value()]
            : zetasql::Value::Null(primary_key[i]->column()->GetType()),
        primary_key[i]->is_descending());
  }
  return key;
}

}  // namespace

zetasql_base::StatusOr<ResolvedReadArg> ResolveReadArg(const ReadArg& read_arg,
                                               const Schema* schema) {
  // Find the table to read from schema.
  auto read_table = schema->FindTable(read_arg.table);
  if (read_table == nullptr) {
    return error::TableNotFound(read_arg.table);
  }

  // Check if index should be read from instead.
  const Index* index = nullptr;
  if (!read_arg.index.empty()) {
    index = schema->FindIndex(read_arg.index);
    if (index == nullptr) {
      return error::IndexNotFound(read_arg.index, read_arg.table);
    }
    if (index->indexed_table() != read_table) {
      return error::IndexTableDoesNotMatchBaseTable(
          read_table->Name(), index->indexed_table()->Name(), index->Name());
    }
    read_table = index->index_data_table();
  }

  // Find the columns to read from schema.
  std::vector<const Column*> columns;
  for (const auto& column_name : read_arg.columns) {
    const Column* column = read_table->FindColumn(column_name);
    if (column == nullptr) {
      if (index == nullptr) {
        return error::ColumnNotFound(read_table->Name(), column_name);
      }
      // Check if column exists in base table and not in index table.
      const Column* base_column =
          index->indexed_table()->FindColumn(column_name);
      if (base_column == nullptr) {
        return error::ColumnNotFound(index->indexed_table()->Name(),
                                     column_name);
      }
      return error::ColumnNotFoundInIndex(
          index->Name(), index->indexed_table()->Name(), column_name);
    }
    columns.push_back(column);
  }

  // Convert key set to canonicalized key ranges.
  std::vector<KeyRange> key_ranges;
  CanonicalizeKeySetForTable(read_arg.key_set, read_table, &key_ranges);

  ResolvedReadArg resolved_read_arg;
  resolved_read_arg.table = read_table;
  resolved_read_arg.key_ranges = std::move(key_ranges);
  resolved_read_arg.columns = columns;
  return resolved_read_arg;
}

zetasql_base::StatusOr<ResolvedMutationOp> ResolveMutationOp(
    const MutationOp& mutation_op, const Schema* schema, absl::Time now) {
  const Table* table = schema->FindTable(mutation_op.table);
  if (table == nullptr) {
    return error::TableNotFound(mutation_op.table);
  }

  ResolvedMutationOp resolved_mutation_op;
  resolved_mutation_op.table = table;
  resolved_mutation_op.type = mutation_op.type;

  if (mutation_op.type == MutationOpType::kDelete) {
    // Invalid commit timestamp key ranges for delete ops need to be caught
    // before CanonicalizeKeySetForTable filters out invalid key ranges with
    // future timestamp(s).
    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeySetForDeleteOp(
        table, mutation_op.key_set, now));
    std::vector<KeyRange> key_ranges;
    CanonicalizeKeySetForTable(mutation_op.key_set, table, &key_ranges);

    for (const KeyRange& key_range : key_ranges) {
      // Transaction store may contain commit timestamp sentinel value until
      // flush, if requested so in a previous mutation. Hence, convert key
      // values to timestamp sentinel value to delete such buffered rows.
      ZETASQL_ASSIGN_OR_RETURN(
          resolved_mutation_op.key_ranges.emplace_back(),
          MaybeSetCommitTimestampSentinel(table->primary_key(), key_range));
    }
  } else {
    ZETASQL_RETURN_IF_ERROR(ValidateColumnsAreNotDuplicate(mutation_op.columns));

    ZETASQL_ASSIGN_OR_RETURN(std::vector<const Column*> columns,
                     GetColumnsByName(table, mutation_op.columns));

    ZETASQL_ASSIGN_OR_RETURN(std::vector<absl::optional<int>> key_indices,
                     ExtractPrimaryKeyIndices(columns, table->primary_key()));

    for (const ValueList& row : mutation_op.rows) {
      ZETASQL_RET_CHECK_EQ(row.size(), columns.size())
          << "MutationOp has difference in size of column and value vectors, "
             "mutation op: "
          << mutation_op.DebugString();

      // Insert, InsertOrUpdate and Replace mutation ops require that all
      // not-null columns be present in the mutation. Note: this check is
      // specifically done before InsertOrUpdate & Replace mutation ops are
      // flattened.
      if (mutation_op.type == MutationOpType::kInsert ||
          mutation_op.type == MutationOpType::kInsertOrUpdate ||
          mutation_op.type == MutationOpType::kReplace) {
        ZETASQL_RETURN_IF_ERROR(ValidateNotNullColumnsPresent(table, columns));
      }

      ZETASQL_RETURN_IF_ERROR(ValidateGeneratedColumnsNotPresent(table, columns));

      ZETASQL_ASSIGN_OR_RETURN(resolved_mutation_op.rows.emplace_back(),
                       MaybeSetCommitTimestampSentinel(columns, row));

      resolved_mutation_op.keys.push_back(ComputeKey(
          resolved_mutation_op.rows.back(), table->primary_key(), key_indices));
    }

    resolved_mutation_op.columns = std::move(columns);
  }

  return resolved_mutation_op;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
