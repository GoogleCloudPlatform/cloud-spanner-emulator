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

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/query_context.h"
#include "backend/query/query_validator.h"
#include "backend/query/queryable_column.h"
#include "backend/query/queryable_table.h"
#include "backend/query/queryable_view.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_node.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

Udf::Determinism AnalyzedVolatilityToDeterminism(
    zetasql::FunctionEnums::Volatility volatility) {
  switch (volatility) {
    case zetasql::FunctionEnums::IMMUTABLE:
      return Udf::Determinism::DETERMINISTIC;
    case zetasql::FunctionEnums::STABLE:
      return Udf::Determinism::NOT_DETERMINISTIC_STABLE;
    case zetasql::FunctionEnums::VOLATILE:
      return Udf::Determinism::NOT_DETERMINISTIC_VOLATILE;
    default:
      return Udf::Determinism::DETERMINISM_UNSPECIFIED;
  };
}

Udf::Determinism ReduceToLeastDeterministic(Udf::Determinism determinism_1,
                                            Udf::Determinism determinism_2) {
  if (determinism_2 == Udf::Determinism::NOT_DETERMINISTIC_VOLATILE) {
    return determinism_2;
  }
  if (determinism_2 == Udf::Determinism::NOT_DETERMINISTIC_STABLE &&
      (determinism_1 == Udf::Determinism::NOT_DETERMINISTIC_STABLE ||
       determinism_1 == Udf::Determinism::DETERMINISTIC ||
       determinism_1 == Udf::Determinism::DETERMINISM_UNSPECIFIED)) {
    return determinism_2;
  }
  if (determinism_2 == Udf::Determinism::DETERMINISTIC) {
    if (determinism_1 == Udf::Determinism::DETERMINISTIC ||
        determinism_1 == Udf::Determinism::DETERMINISM_UNSPECIFIED) {
      return determinism_2;
    }
  };
  return determinism_1;
}

}  // namespace

// A validator that checks column expressions for valid SQL.
class ColumnExpressionValidator : public QueryValidator {
 public:
  ColumnExpressionValidator(
      const Schema* schema, const zetasql::Table* table,
      absl::string_view expression_use,
      absl::flat_hash_set<std::string>* dependent_column_names,
      bool allow_volatile_expression,
      absl::flat_hash_set<const SchemaNode*>* udf_dependencies)
      : QueryValidator(QueryContext{.schema = schema,
                                    .allow_read_write_only_functions = true},
                       /*options=*/nullptr),
        table_(table),
        expression_use_(expression_use),
        dependent_column_names_(dependent_column_names),
        allow_volatile_expression_(allow_volatile_expression),
        udf_dependencies_(udf_dependencies) {}

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
    const Udf* udf = schema()->FindUdf(node->function()->FullName(false));
    if (udf != nullptr) {
      // The schema object UDF is transitive across its own dependencies.
      if (udf->determinism_level() != Udf::Determinism::DETERMINISTIC &&
          !allow_volatile_expression_) {
        return error::NonDeterministicFunctionInColumnExpression(
            udf->Name(), expression_use_);
      }
      udf_dependencies_->insert(udf);
    } else {
      if (node->function()->function_options().volatility !=
              zetasql::FunctionEnums::IMMUTABLE &&
          !allow_volatile_expression_) {
        return error::NonDeterministicFunctionInColumnExpression(
            node->function()->SQLName(), expression_use_);
      }
    }

    return absl::OkStatus();
  }

 private:
  const zetasql::Table* table_;
  absl::string_view expression_use_;
  absl::flat_hash_set<std::string>* dependent_column_names_;
  bool allow_volatile_expression_;
  absl::flat_hash_set<const SchemaNode*>* udf_dependencies_;
};

// A validator that checks view definitions for valid SQL.
class ViewDefinitionValidator : public QueryValidator {
 public:
  // The dependencies returned in `dependencies` are not transitive. i.e. they
  // are only the direct dependencies of the view definition being validated.
  ViewDefinitionValidator(const Schema* schema,
                          const zetasql::LanguageOptions& language_options,
                          absl::flat_hash_set<const SchemaNode*>* dependencies)
      : QueryValidator({.schema = schema}, /*extracted_options=*/nullptr,
                       /*language_options=*/language_options),
        dependencies_(dependencies) {}

 private:
  absl::Status VisitResolvedWithScan(
      const zetasql::ResolvedWithScan* node) override {
    return error::WithViewsAreNotSupported();
  }

  absl::Status VisitResolvedTableScan(
      const zetasql::ResolvedTableScan* scan) override {
    // Visit the entire tree for the scan first, validating it and collecting
    // any references to indexes. Collect the references after the view query
    // has been determined to be valid.
    ZETASQL_RETURN_IF_ERROR(QueryValidator::VisitResolvedTableScan(scan));
    // The 'catalog table' referenced in the resolved AST could be a table or a
    // view.
    auto catalog_table = scan->table();
    if (catalog_table->Is<backend::QueryableTable>()) {
      dependencies_->insert(
          catalog_table->GetAs<backend::QueryableTable>()->wrapped_table());
    } else if (catalog_table->Is<backend::QueryableView>()) {
      dependencies_->insert(
          catalog_table->GetAs<backend::QueryableView>()->wrapped_view());
    } else {
      // This should not happen. A view referencing a non-existent dependency
      // should fail analaysis.
      ZETASQL_RET_CHECK_FAIL() << "Dependency not found: " << catalog_table->Name();
    }

    // Add the column dependencies for the view.
    // We analyze the view with prune_unused_columns=true. This should result
    // in the resolved scan containing only the columns that are referenced in
    // the view.
    const auto& used_columns = scan->column_index_list();
    for (auto column_index : used_columns) {
      auto catalog_column = catalog_table->GetColumn(column_index);
      ZETASQL_RET_CHECK_NE(catalog_column, nullptr)
          << "Referenced column "
          << scan->column_list()[column_index].DebugString() << " not found in "
          << catalog_table->Name();
      if (catalog_column->Is<backend::QueryableColumn>()) {
        dependencies_->insert(catalog_column->GetAs<backend::QueryableColumn>()
                                  ->wrapped_column());
      }
    }

    // Also add any indexes used as dependencies
    for (const auto* index : indexes_used()) {
      ZETASQL_RET_CHECK_NE(index, nullptr);
      dependencies_->insert(index);
    }

    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override {
    ZETASQL_RETURN_IF_ERROR(QueryValidator::VisitResolvedFunctionCall(node));

    const Udf* udf =
        schema()->FindUdf(node->function()->FullName(/*include_group=*/false));
    if (udf != nullptr) {
      dependencies_->insert(udf);
    }

    return absl::OkStatus();
  }

 private:
  absl::flat_hash_set<const SchemaNode*>* dependencies_;
};

// A validator that checks udf definitions for valid SQL.
class UdfDefinitionValidator : public QueryValidator {
 public:
  // The dependencies returned in `dependencies` are not transitive. i.e. they
  // are only the direct dependencies of the view definition being validated.
  UdfDefinitionValidator(const Schema* schema,
                         const zetasql::LanguageOptions& language_options,
                         absl::flat_hash_set<const SchemaNode*>* dependencies,
                         Udf::Determinism* determinism_level)
      : QueryValidator({.schema = schema}, /*extracted_options=*/nullptr,
                       /*language_options=*/language_options),
        dependencies_(dependencies),
        determinism_level_(determinism_level) {}

 private:
  absl::Status VisitResolvedWithScan(
      const zetasql::ResolvedWithScan* node) override {
    return error::WithViewsAreNotSupported();
  }

  absl::Status VisitResolvedTableScan(
      const zetasql::ResolvedTableScan* scan) override {
    // Visit the entire tree for the scan first, validating it and collecting
    // any references to indexes. Collect the references after the udf query
    // has been determined to be valid.
    ZETASQL_RETURN_IF_ERROR(QueryValidator::VisitResolvedTableScan(scan));
    // The 'catalog table' referenced in the resolved AST could be a table or a
    // view.
    auto catalog_table = scan->table();
    if (catalog_table->Is<backend::QueryableTable>()) {
      dependencies_->insert(
          catalog_table->GetAs<backend::QueryableTable>()->wrapped_table());
    } else if (catalog_table->Is<backend::QueryableView>()) {
      dependencies_->insert(
          catalog_table->GetAs<backend::QueryableView>()->wrapped_view());
    } else {
      // This should not happen. A udf referencing a non-existent dependency
      // should fail analaysis.
      ZETASQL_RET_CHECK_FAIL() << "Dependency not found: " << catalog_table->Name();
    }

    // Add the column dependencies for the udf.
    // We analyze the udf with prune_unused_columns=true. This should result
    // in the resolved scan containing only the columns that are referenced in
    // the udf.
    const auto& used_columns = scan->column_index_list();
    for (auto column_index : used_columns) {
      auto catalog_column = catalog_table->GetColumn(column_index);
      ZETASQL_RET_CHECK_NE(catalog_column, nullptr)
          << "Referenced column "
          << scan->column_list()[column_index].DebugString() << " not found in "
          << catalog_table->Name();
      if (catalog_column->Is<backend::QueryableColumn>()) {
        dependencies_->insert(catalog_column->GetAs<backend::QueryableColumn>()
                                  ->wrapped_column());
      }
    }

    // Also add any indexes used as dependencies
    for (const auto* index : indexes_used()) {
      ZETASQL_RET_CHECK_NE(index, nullptr);
      dependencies_->insert(index);
    }

    return absl::OkStatus();
  }

 protected:
  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override {
    ZETASQL_RETURN_IF_ERROR(QueryValidator::VisitResolvedFunctionCall(node));

    // ZETASQL_VLOG IF THIS UDF IS ALWAYS THE SAME AS THE NODE ONE
    const Udf* udf = schema()->FindUdf(node->function()->FullName(false));
    if (udf != nullptr) {
      *determinism_level_ = ReduceToLeastDeterministic(
          *determinism_level_, udf->determinism_level());
      dependencies_->insert(udf);
    } else {
      *determinism_level_ = ReduceToLeastDeterministic(
          *determinism_level_,
          AnalyzedVolatilityToDeterminism(
              node->function()->function_options().volatility));
    }
    return absl::OkStatus();
  }

 private:
  absl::flat_hash_set<const SchemaNode*>* dependencies_;
  Udf::Determinism* determinism_level_;
};

absl::Status AnalyzeColumnExpression(
    absl::string_view expression, const zetasql::Type* target_type,
    const Table* table, const Schema* schema,
    zetasql::TypeFactory* type_factory,
    const std::vector<zetasql::SimpleTable::NameAndType>& name_and_types,
    absl::string_view expression_use,
    absl::flat_hash_set<std::string>* dependent_column_names,
    absl::flat_hash_set<const SchemaNode*>* dependent_sequences,
    bool allow_volatile_expression,
    absl::flat_hash_set<const SchemaNode*>* udf_dependencies) {
  zetasql::SimpleTable simple_table(table->Name(), name_and_types);
  zetasql::AnalyzerOptions options = MakeGoogleSqlAnalyzerOptions();
  // ZetaSQL rewriting could rewrite scalar expressions into subquery.
  // Disable all default enabled rewriting to check the original shape of
  // user provided expression and ensure forward compatibility.
  auto enabled_rewrites = options.enabled_rewrites();
  for (auto enabled_rewrite : enabled_rewrites) {
    options.enable_rewrite(enabled_rewrite, false);
  }

  for (const auto& name_and_type : name_and_types) {
    ZETASQL_RETURN_IF_ERROR(
        options.AddExpressionColumn(name_and_type.first, name_and_type.second));
  }
  std::unique_ptr<const zetasql::AnalyzerOutput> output;
  FunctionCatalog function_catalog(type_factory);
  Catalog catalog(schema, &function_catalog, type_factory);

  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeExpressionForAssignmentToType(
      expression, options, &catalog, type_factory, target_type, &output));

  ColumnExpressionValidator validator(
      schema, &simple_table, expression_use, dependent_column_names,
      allow_volatile_expression, udf_dependencies);
  ZETASQL_RETURN_IF_ERROR(output->resolved_expr()->Accept(&validator));
  if (output->resolved_expr()->GetTreeDepth() >
      limits::kColumnExpressionMaxDepth) {
    return error::ColumnExpressionMaxDepthExceeded(
        output->resolved_expr()->GetTreeDepth(),
        limits::kColumnExpressionMaxDepth);
  }
  if (dependent_sequences != nullptr &&
      !validator.dependent_sequences().empty()) {
    *dependent_sequences = validator.dependent_sequences();
  }

  return absl::OkStatus();
}

absl::Status AnalyzeViewDefinition(
    absl::string_view view_name, absl::string_view view_definition,
    const Schema* schema, zetasql::TypeFactory* type_factory,
    std::vector<View::Column>* output_columns,
    absl::flat_hash_set<const SchemaNode*>* dependencies) {
  auto body = absl::Substitute("CREATE VIEW `$0` SQL SECURITY INVOKER AS $1",
                               view_name, view_definition);

  // Analyze the view definition.
  auto analyzer_options =
      MakeGoogleSqlAnalyzerOptionsForViewsAndFunctions(schema->dialect());
  analyzer_options.set_prune_unused_columns(true);
  FunctionCatalog function_catalog(
      type_factory, kCloudSpannerEmulatorFunctionCatalogName, schema);
  Catalog catalog(schema, &function_catalog, type_factory, analyzer_options);
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;
  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(body, analyzer_options, &catalog,
                                              type_factory, &analyzer_output));

  // Check the view definition for only allowed elements.
  const zetasql::ResolvedCreateViewStmt* create_view_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<zetasql::ResolvedCreateViewStmt>();
  ViewDefinitionValidator validator(schema, analyzer_options.language(),
                                    dependencies);
  ZETASQL_RETURN_IF_ERROR(create_view_stmt->query()->Accept(&validator));
  for (const auto& c : create_view_stmt->output_column_list()) {
    output_columns->emplace_back(View::Column{c->name(), c->column().type()});
  }

  for (const SchemaNode* sequence : validator.dependent_sequences()) {
    dependencies->insert(sequence);
  }

  return absl::OkStatus();
}

absl::Status AnalyzeUdfDefinition(
    absl::string_view udf_name, absl::string_view param_list,
    absl::string_view udf_definition, const Schema* schema,
    zetasql::TypeFactory* type_factory,
    absl::flat_hash_set<const SchemaNode*>* dependencies,
    std::unique_ptr<zetasql::FunctionSignature>* function_signature,
    Udf::Determinism* determinism_level) {
  auto body =
      absl::Substitute("CREATE FUNCTION `$0`($1) SQL SECURITY INVOKER AS ($2)",
                       udf_name, param_list, udf_definition);
  // Analyze the udf definition.
  auto analyzer_options =
      MakeGoogleSqlAnalyzerOptionsForViewsAndFunctions(schema->dialect());
  analyzer_options.set_prune_unused_columns(true);
  FunctionCatalog function_catalog(
      type_factory, kCloudSpannerEmulatorFunctionCatalogName, schema);
  Catalog catalog(schema, &function_catalog, type_factory, analyzer_options);
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;
  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(body, analyzer_options, &catalog,
                                              type_factory, &analyzer_output));

  // Check the udf definition for only allowed elements.
  const zetasql::ResolvedCreateFunctionStmt* create_function_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<zetasql::ResolvedCreateFunctionStmt>();

  UdfDefinitionValidator validator(schema, analyzer_options.language(),
                                   dependencies, determinism_level);
  ZETASQL_RETURN_IF_ERROR(
      create_function_stmt->function_expression()->Accept(&validator));
  for (const SchemaNode* sequence : validator.dependent_sequences()) {
    dependencies->insert(sequence);
  }
  *function_signature = absl::make_unique<zetasql::FunctionSignature>(
      create_function_stmt->signature());

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
