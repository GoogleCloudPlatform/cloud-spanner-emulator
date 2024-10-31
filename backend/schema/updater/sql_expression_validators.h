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

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/function_signature.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_node.h"

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
    bool allow_volatile_expression,
    absl::flat_hash_set<const SchemaNode*>* udf_dependencies);

// Analyzes the view definition in `view_definition`. Returns the
// table, column and other view dependencies in `dependencies` and
// the analyzed view's output columns in `output_columns`.
absl::Status AnalyzeViewDefinition(
    absl::string_view view_name, absl::string_view view_definition,
    const Schema* schema, zetasql::TypeFactory* type_factory,
    std::vector<View::Column>* output_columns,
    absl::flat_hash_set<const SchemaNode*>* dependencies);

// Analyzes the UDF definition in `udf_definition`. Returns the
// table, column and other view dependencies in `dependencies` and
// the analyzed UDF's signature in `function_signature`.
absl::Status AnalyzeUdfDefinition(
    absl::string_view udf_name, absl::string_view param_list,
    absl::string_view udf_definition, const Schema* schema,
    zetasql::TypeFactory* type_factory,
    absl::flat_hash_set<const SchemaNode*>* dependencies,
    std::unique_ptr<zetasql::FunctionSignature>* function_signature,
    Udf::Determinism* determinism_level);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SQL_EXPRESSION_VALIDATORS_H_
