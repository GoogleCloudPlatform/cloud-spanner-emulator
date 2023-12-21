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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SQL_EXPRESSION_VALIDATORS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SQL_EXPRESSION_VALIDATORS_H_

#include <string>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "backend/query/query_validator.h"
#include "backend/schema/catalog/schema.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Analyzes a SQL expression in `expression` for a column.
absl::Status AnalyzeColumnExpression(
    absl::string_view expression, const zetasql::Type* target_type,
    const Table* table, const Schema* schema,
    zetasql::TypeFactory* type_factory,
    const std::vector<zetasql::SimpleTable::NameAndType>& name_and_types,
    absl::string_view expression_use,
    absl::flat_hash_set<std::string>* dependent_column_names,
    absl::flat_hash_set<const SchemaNode*>* dependent_sequences,
    bool allow_volatile_expression);

// Analyzes the view definition in `view_definition`. Returns the
// table, column and other view dependencies in `dependencies` and
// the analyzed view's output columns in `output_columns`.
absl::Status AnalyzeViewDefinition(
    absl::string_view view_name, absl::string_view view_definition,
    const Schema* schema, zetasql::TypeFactory* type_factory,
    std::vector<View::Column>* output_columns,
    absl::flat_hash_set<const SchemaNode*>* dependencies);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SQL_EXPRESSION_VALIDATORS_H_
