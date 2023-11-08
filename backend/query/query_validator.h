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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_VALIDATOR_H_

#include <stdbool.h>

#include <utility>

#include "zetasql/public/language_options.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/feature_filter/sql_features_view.h"
#include "backend/query/query_context.h"
#include "backend/query/query_engine_options.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Implements ResolvedASTVisitor to validate various nodes in an AST and
// collect information of interest (such as index names).
class QueryValidator : public zetasql::ResolvedASTVisitor {
 public:
  explicit QueryValidator(const QueryContext context,
                          QueryEngineOptions* extracted_options = nullptr,
                          const zetasql::LanguageOptions language_options =
                              MakeGoogleSqlLanguageOptions())
      : context_(std::move(context)),
        language_options_(std::move(language_options)),
        sql_features_(SqlFeaturesView()),
        extracted_options_(extracted_options) {}

  // Returns the list of indexes used by the query being validated. Must
  // be called after the entire tree has been visited.
  const absl::flat_hash_set<const Index*>& indexes_used() const {
    return indexes_used_;
  }

 protected:
  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override;

  absl::Status VisitResolvedQueryStmt(
      const zetasql::ResolvedQueryStmt* node) override;

  absl::Status VisitResolvedLiteral(
      const zetasql::ResolvedLiteral* node) override;

  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override;

  absl::Status VisitResolvedAggregateFunctionCall(
      const zetasql::ResolvedAggregateFunctionCall* node) override;

  absl::Status VisitResolvedCast(const zetasql::ResolvedCast* node) override;

  absl::Status VisitResolvedSampleScan(
      const zetasql::ResolvedSampleScan* node) override;

  const Schema* schema() const { return context_.schema; }

 private:
  // Validates the child hint nodes of `node`.
  absl::Status ValidateHints(const zetasql::ResolvedNode* node);

  // Returns an OK if `name` is a supported hint name for nodes with kind
  // `node_kind`; otherwise, returns an invalid argument error.
  absl::Status CheckSpannerHintName(
      absl::string_view name,
      const zetasql::ResolvedNodeKind node_kind) const;

  // Returns an OK if `name` is a supported Emulator-only hint name for nodes
  // with kind `node_kind`; otherwise, returns an invalid argument error.
  absl::Status CheckEmulatorHintName(
      absl::string_view name,
      const zetasql::ResolvedNodeKind node_kind) const;

  // Returns an OK if `value` represents a valid value for hints with name
  // `name` specified on a node of kind `node_kind`; otherwise, returns an
  // invalid argument error. `hint_map` is initialized to a mapping of
  // hint_name->hint_value of all the hints specified on the node.
  absl::Status CheckHintValue(
      absl::string_view name, const zetasql::Value& value,
      const zetasql::ResolvedNodeKind node_kind,
      const absl::flat_hash_map<absl::string_view, zetasql::Value>& hint_map);

  // Extracts query engine options from any Emulator-specific hints
  // 'node_hint_map' for a node.
  absl::Status ExtractEmulatorOptionsForNode(
      const absl::flat_hash_map<absl::string_view, zetasql::Value>&
          node_hint_map) const;

  const QueryContext context_;

  const zetasql::LanguageOptions language_options_;

  const SqlFeaturesView sql_features_;

  // List of indexes used by the query being validated.
  absl::flat_hash_set<const Index*> indexes_used_;

  // Options for the query engine that are extracted through user-specified
  // hints.
  QueryEngineOptions* extracted_options_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_VALIDATOR_H_
