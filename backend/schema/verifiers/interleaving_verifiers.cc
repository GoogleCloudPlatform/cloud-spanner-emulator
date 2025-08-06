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

#include "backend/schema/verifiers/interleaving_verifiers.h"

#include <memory>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/storage/iterator.h"
#include "backend/storage/storage.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status VerifyInterleaveInParentTableRowsExist(
    const Table* parent, const Table* child,
    const SchemaValidationContext* context) {
  FunctionCatalog function_catalog(context->type_factory());
  zetasql::AnalyzerOptions analyzer_options = MakeGoogleSqlAnalyzerOptions(
      context->validated_new_schema()->default_time_zone());
  Catalog catalog(context->validated_new_schema(), &function_catalog,
                  context->type_factory(), analyzer_options);

  const Storage* storage = context->storage();
  absl::Time timestamp = context->pending_commit_timestamp();
  std::unique_ptr<StorageIterator> iterator;
  std::vector<ColumnID> column_ids = GetColumnIDs(child->columns());
  std::vector<ColumnID> parent_column_ids = GetColumnIDs(parent->columns());
  ZETASQL_RETURN_IF_ERROR(storage->Read(timestamp, child->id(), KeyRange::All(),
                                column_ids, &iterator));

  // Loop through every row of the child table and look for a parent row.
  while (iterator->Next()) {
    // Compute the parent key as prefix of the child key.
    Key parent_key = iterator->Key().Prefix(parent->primary_key().size());
    std::vector<zetasql::Value> parent_values;
    absl::Status status = storage->Lookup(timestamp, parent->id(), parent_key,
                                          parent_column_ids, &parent_values);
    if (!status.ok()) {
      if (absl::IsNotFound(status)) {
        return error::InterleavingParentChildRowExistenceConstraintValidation(
            parent->Name(), child->Name(), parent_key.DebugString());
      }
      return status;
    }
  }
  return iterator->Status();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
