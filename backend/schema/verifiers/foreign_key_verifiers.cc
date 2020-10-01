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

#include "backend/schema/verifiers/foreign_key_verifiers.h"

#include <algorithm>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

std::vector<ColumnID> DataColumnIds(const Table* data_table, int column_count) {
  std::vector<ColumnID> column_ids;
  std::transform(
      data_table->primary_key().begin(), data_table->primary_key().end(),
      std::back_inserter(column_ids),
      [](const KeyColumn* key_column) { return key_column->column()->id(); });
  return std::vector<ColumnID>(column_ids.begin(),
                               column_ids.begin() + column_count);
}

}  // namespace

absl::Status VerifyForeignKeyData(const ForeignKey* foreign_key,
                                  const SchemaValidationContext* context) {
  const Storage* storage = context->storage();
  absl::Time timestamp = context->pending_commit_timestamp();
  int column_count = foreign_key->referencing_columns().size();
  TableID referenced_data_table_id = foreign_key->referenced_data_table()->id();
  std::unique_ptr<StorageIterator> referencing_iterator;
  ZETASQL_RETURN_IF_ERROR(storage->Read(
      timestamp, foreign_key->referencing_data_table()->id(), KeyRange::All(),
      DataColumnIds(foreign_key->referencing_data_table(), column_count),
      &referencing_iterator));
  while (referencing_iterator->Next()) {
    Key constraint_key(std::vector<zetasql::Value>(
        referencing_iterator->Key().column_values().begin(),
        referencing_iterator->Key().column_values().begin() + column_count));
    std::unique_ptr<StorageIterator> referenced_iterator;
    ZETASQL_RETURN_IF_ERROR(storage->Read(timestamp, referenced_data_table_id,
                                  KeyRange::Point(constraint_key), {},
                                  &referenced_iterator));
    if (!referenced_iterator->Next() && referenced_iterator->Status().ok()) {
      return error::ForeignKeyReferencedKeyNotFound(
          foreign_key->Name(), foreign_key->referencing_table()->Name(),
          foreign_key->referenced_table()->Name(),
          constraint_key.DebugString());
    }
    ZETASQL_RETURN_IF_ERROR(referenced_iterator->Status());
  }
  return referencing_iterator->Status();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
