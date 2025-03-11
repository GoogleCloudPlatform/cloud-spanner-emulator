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
#include "absl/types/span.h"
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

bool IsSearchQueryAllowed(const QueryEngineOptions* options,
                          const QueryContext& context);

// Returns true if the given node or any of its descendants is a
// ResolvedLockMode node which makes this a query that uses SELECT FOR UPDATE.
bool IsSelectForUpdateQuery(const zetasql::ResolvedNode& node);

// QueryFeatures provides information about features used in a query.
struct QueryFeatures {
  // If true, we're in a query with a FOR UPDATE clause. Required for returning
  // appropriate errors when combined with other features.
  bool has_for_update = false;

  // If true, it affects errors returned by FOR UPDATE queries. The value of
  // the hint is ignored even if it was specified since the emulator doesn't
  // acquire locks.
  bool has_lock_scanned_ranges = false;
};

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

  void set_in_partition_query(bool in_partition_query) {
    in_partition_query_ = in_partition_query;
  }

  const absl::flat_hash_set<const SchemaNode*>& dependent_sequences() {
    return dependent_sequences_;
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

  absl::Status VisitResolvedInsertStmt(
      const zetasql::ResolvedInsertStmt* node) override;

  absl::Status VisitResolvedUpdateStmt(
      const zetasql::ResolvedUpdateStmt* node) override;

  absl::Status VisitResolvedDeleteStmt(
      const zetasql::ResolvedDeleteStmt* node) override;

  absl::Status VisitResolvedTableScan(
      const zetasql::ResolvedTableScan* node) override;

  const Schema* schema() const { return context_.schema; }

  bool IsSequenceFunction(const zetasql::ResolvedFunctionCall* node) const;

  absl::Status ValidateSequenceFunction(
      const zetasql::ResolvedFunctionCall* node);

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

  absl::Status CheckSearchFunctionsAreAllowed(
      const zetasql::ResolvedFunctionCall& function_call);

  // Validate that the presence of a ResolvedLockMode node (FOR UPDATE in a
  // SELECT query) is valid in this context.
  absl::Status CheckTableScanLockModeAllowed(const QueryEngineOptions* options,
                                             const QueryContext& context) const;

  // Check a node's hint map and extracts query engine options from any Spanner
  // hints
  absl::Status ExtractSpannerOptionsForNode(
      const absl::flat_hash_map<absl::string_view, zetasql::Value>&
          node_hint_map);

  // Extracts query engine options from any Emulator-specific hints
  // 'node_hint_map' for a node.
  absl::Status ExtractEmulatorOptionsForNode(
      const absl::flat_hash_map<absl::string_view, zetasql::Value>&
          node_hint_map) const;

  // Enforces restrictions on reading tables/columns containing pending commit
  // timestamps. If `access_list` is non-empty it will be used to ignore columns
  // which are not read.
  absl::Status CheckPendingCommitTimestampReads(
      const zetasql::ResolvedTableScan* table_scan,
      absl::Span<const zetasql::ResolvedStatement::ObjectAccess> access_list =
          {});

  // Returns true if the function is allowed in read-write transactions
  // only. E.g. GET_NEXT_SEQUENCE_VALUE().
  bool IsReadWriteOnlyFunction(absl::string_view name) const;

  const QueryContext context_;

  const zetasql::LanguageOptions language_options_;

  bool in_partition_query_ = false;
  const SqlFeaturesView sql_features_;

  // List of indexes used by the query being validated.
  absl::flat_hash_set<const Index*> indexes_used_;

  // List of sequences used by the query being validated.
  absl::flat_hash_set<const SchemaNode*> dependent_sequences_;

  // Table scans from DML statements. Depending on context, the column list
  // on these scans may include columns which are written, but not read. This
  // becomes relevant when we enforce pending commit timestamp restrictions.
  absl::flat_hash_set<const zetasql::ResolvedTableScan*> dml_table_scans_;

  // Options for the query engine that are extracted through user-specified
  // hints.
  QueryEngineOptions* extracted_options_;

  // Features used by the query. Required to verify if a feature used in one
  // part of a query can be combined with features used in another part of the
  // query.
  QueryFeatures query_features_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_VALIDATOR_H_
