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
#include "zetasql/resolved_ast/resolved_ast.h"
#include "backend/query/query_validator.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A validator that checks column expressions for valid SQL.
class ColumnExpressionValidator : public QueryValidator {
 public:
  ColumnExpressionValidator(
      const Schema* schema, const zetasql::Table* table,
      absl::string_view expression_use,
      absl::flat_hash_set<std::string>* dependent_column_names,
      bool allow_volatile_expression)
      : QueryValidator(schema, /*options=*/nullptr),
        table_(table),
        expression_use_(expression_use),
        dependent_column_names_(dependent_column_names),
        allow_volatile_expression_(allow_volatile_expression) {}

  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override {
    if (node->IsScan() ||
        node->node_kind() == zetasql::RESOLVED_SUBQUERY_EXPR) {
      return error::NonScalarExpressionInColumnExpression(expression_use_);
    }
    if (node->node_kind() == zetasql::RESOLVED_EXPRESSION_COLUMN) {
      std::string column_name =
          node->GetAs<zetasql::ResolvedExpressionColumn>()->name();
      const zetasql::Column* column = table_->FindColumnByName(column_name);
      ZETASQL_RET_CHECK_NE(column, nullptr);
      dependent_column_names_->insert(column->Name());
    }
    return QueryValidator::DefaultVisit(node);
  }

 protected:
  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override {
    // The validation order matters here.
    // Need to invoke the parent visitor first since some higher level
    // validation should precede the deterministic function check. For example,
    // using pending_commit_timestamp() in generated column at CREATE TABLE
    // should return error due to that function only being allowed in INSERT or
    // UPDATE.
    ZETASQL_RETURN_IF_ERROR(QueryValidator::VisitResolvedFunctionCall(node));
    if (!allow_volatile_expression_ &&
        node->function()->function_options().volatility !=
            zetasql::FunctionEnums::IMMUTABLE) {
      return error::NonDeterministicFunctionInColumnExpression(
          node->function()->SQLName(), expression_use_);
    }
    return absl::OkStatus();
  }

 private:
  const zetasql::Table* table_;
  absl::string_view expression_use_;
  absl::flat_hash_set<std::string>* dependent_column_names_;
  bool allow_volatile_expression_;
};

// A validator that checks view definitions for valid SQL.
class ViewDefinitionValidator : public QueryValidator {
 public:
  // The dependencies returned in `dependencies` are not transitive. i.e. they
  // are only the direct dependencies of the view definition being validated.
  ViewDefinitionValidator(const Schema* schema,
                          const zetasql::LanguageOptions& language_options,
                          absl::flat_hash_set<const SchemaNode*>* dependencies)
      : QueryValidator(schema, /*extracted_options=*/nullptr,
                       /*language_options=*/language_options),
        dependencies_(dependencies) {}

 private:
  // TODO: For columns, track dependencies on ResolvedColumn node.
  absl::Status VisitResolvedTableScan(
      const zetasql::ResolvedTableScan* scan) override {
    // The 'catalog table' referenced in the resolved AST could be a table or a
    // view.
    auto catalog_table_name = scan->table()->Name();
    // We currently don't have namespace support so using the unqualified name
    // is fine.
    if (auto table = schema()->FindTable(catalog_table_name);
        table != nullptr) {
      dependencies_->insert(table);
    } else if (auto view = schema()->FindView(catalog_table_name);
               view != nullptr) {
      dependencies_->insert(view);
    } else {
      // This should not happen. A view referencing a non-existent dependency
      // should fail analaysis.
      ZETASQL_RET_CHECK_FAIL() << "Dependency not found: " << catalog_table_name;
    }
    return QueryValidator::VisitResolvedTableScan(scan);
  }

 private:
  absl::flat_hash_set<const SchemaNode*>* dependencies_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SQL_EXPRESSION_VALIDATORS_H_
