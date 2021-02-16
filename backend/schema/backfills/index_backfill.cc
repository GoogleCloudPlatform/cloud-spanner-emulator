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

#include "backend/schema/backfills/index_backfill.h"

#include <memory>
#include <queue>

#include "zetasql/public/functions/string.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "backend/common/ids.h"
#include "backend/common/indexing.h"
#include "backend/common/rows.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/storage/iterator.h"
#include "common/errors.h"
#include "common/limits.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status BackfillIndex(const Index* index,
                           const SchemaValidationContext* context) {
  absl::Span<const Column* const> base_columns =
      index->indexed_table()->columns();
  absl::Span<const Column* const> index_columns =
      index->index_data_table()->columns();
  std::vector<ColumnID> base_column_ids = GetColumnIDs(base_columns);
  std::vector<ColumnID> index_column_ids = GetColumnIDs(index_columns);

  // TODO: Use actions framework for index backfills.
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(context->storage()->Read(
      context->pending_commit_timestamp(), index->indexed_table()->id(),
      KeyRange::All(), base_column_ids, &itr));

  // List of index keys used for verifying index uniqueness.
  std::set<Key> index_keys;
  while (itr->Next()) {
    std::vector<zetasql::Value> row_values;
    row_values.reserve(itr->NumColumns());
    for (int i = 0; i < itr->NumColumns(); ++i) {
      // Storage returns invalid values if a value is not present, in which case
      // we convert it into a typed NULL.
      row_values.emplace_back(
          itr->ColumnValue(i).is_valid()
              ? itr->ColumnValue(i)
              : zetasql::Value::Null(base_columns[i]->GetType()));
    }

    // Compute the index key and column values.
    Row base_row = MakeRow(base_columns, row_values);
    // Backfill should return failed precondition error for invalid index keys.
    ZETASQL_ASSIGN_OR_RETURN(Key index_data_table_key, ComputeIndexKey(base_row, index),
                     _.SetErrorCode(absl::StatusCode::kFailedPrecondition));
    ValueList index_values = ComputeIndexValues(base_row, index);
    if (ShouldFilterIndexKey(index, index_data_table_key)) {
      continue;
    }

    // Check uniqueness constraints.
    if (index->is_unique()) {
      Key index_key = index_data_table_key.Prefix(index->key_columns().size());
      if (index_keys.find(index_key) != index_keys.end()) {
        return error::UniqueIndexViolationOnIndexCreation(
            index->Name(), index_key.DebugString());
      }
      // Add key to current list of index keys.
      index_keys.insert(index_key);
    }

    // Insert the new row in the index.
    ZETASQL_RETURN_IF_ERROR(context->storage()->Write(
        context->pending_commit_timestamp(), index->index_data_table()->id(),
        index_data_table_key, index_column_ids, index_values));
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
