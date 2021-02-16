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

#include "backend/schema/verifiers/check_constraint_verifiers.h"

#include <vector>

#include "absl/status/status.h"
#include "backend/actions/check_constraint.h"
#include "backend/datamodel/key_range.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status VerifyCheckConstraintData(const CheckConstraint* check_constraint,
                                       const SchemaValidationContext* context) {
  FunctionCatalog function_catalog(context->type_factory());
  Catalog catalog(context->new_schema(), &function_catalog);
  CheckConstraintVerifier verifier(check_constraint, &catalog);

  const Table* table = check_constraint->table();
  const Storage* storage = context->storage();
  absl::Time timestamp = context->pending_commit_timestamp();
  std::unique_ptr<StorageIterator> iterator;
  std::vector<ColumnID> column_ids = GetColumnIDs(table->columns());
  ZETASQL_RETURN_IF_ERROR(storage->Read(timestamp, table->id(), KeyRange::All(),
                                column_ids, &iterator));

  // Loop through every row of the table and validate the check constraints.
  while (iterator->Next()) {
    zetasql::ParameterValueMap row_column_values;
    for (int i = 0; i < iterator->NumColumns(); ++i) {
      // Storage returns invalid values if a value is not present, in which case
      // we convert it into a typed NULL.
      row_column_values[table->columns()[i]->Name()] =
          iterator->ColumnValue(i).is_valid()
              ? iterator->ColumnValue(i)
              : zetasql::Value::Null(table->columns()[i]->GetType());
    }
    ZETASQL_RETURN_IF_ERROR(verifier.VerifyRow(row_column_values, iterator->Key()));
  }
  return iterator->Status();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
