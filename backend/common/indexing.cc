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

#include "backend/common/indexing.h"

#include "zetasql/base/statusor.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status ValidateKeySizeForIndex(const Index* index, const Key& key) {
  int64_t key_size = key.LogicalSizeInBytes();
  if (key_size > limits::kMaxKeySizeBytes) {
    return error::IndexKeyTooLarge(index->Name(), key_size,
                                   limits::kMaxKeySizeBytes);
  }
  return absl::OkStatus();
}

}  // namespace

zetasql_base::StatusOr<Key> ComputeIndexKey(const Row& base_row, const Index* index) {
  // Columns must be added to the key for each column in index data table
  // primary key.
  Key key;
  for (const auto& key_column : index->index_data_table()->primary_key()) {
    key.AddColumn(
        GetColumnValueOrNull(base_row, key_column->column()->source_column()),
        key_column->is_descending());
  }
  ZETASQL_RETURN_IF_ERROR(ValidateKeySizeForIndex(index, key));
  return key;
}

ValueList ComputeIndexValues(const Row& base_row, const Index* index) {
  ValueList values;
  for (const Column* column : index->index_data_table()->columns()) {
    values.push_back(GetColumnValueOrNull(base_row, column->source_column()));
  }
  return values;
}

bool ShouldFilterIndexKey(const Index* index, const Key& key) {
  if (!index->is_null_filtered()) {
    return false;
  }

  // Cloud Spanner only checks index key columns for null filtering.
  for (int i = 0; i < index->key_columns().size(); ++i) {
    if (key.ColumnValue(i).is_null()) {
      return true;
    }
  }
  return false;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
