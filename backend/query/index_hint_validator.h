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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INDEX_HINT_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INDEX_HINT_VALIDATOR_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Checks if an index hint specified on a table scan is valid.
class IndexHintValidator : public zetasql::ResolvedASTVisitor {
 public:
  IndexHintValidator(const Schema* schema,
                     bool disable_null_filtered_index_check = false)
      : schema_(schema),
        disable_null_filtered_index_check_(disable_null_filtered_index_check) {}

 private:
  absl::Status VisitResolvedQueryStmt(
      const zetasql::ResolvedQueryStmt* stmt) final;

  absl::Status VisitResolvedInsertStmt(
      const zetasql::ResolvedInsertStmt* stmt) final;

  absl::Status VisitResolvedUpdateStmt(
      const zetasql::ResolvedUpdateStmt* stmt) final;

  absl::Status VisitResolvedDeleteStmt(
      const zetasql::ResolvedDeleteStmt* stmt) final;

  // Validates that all the index hints used in the query can be applied to
  // serving the tables.
  absl::Status ValidateIndexesForTables();

  // To collect the 'force_index' hints from all table scans.
  absl::Status VisitResolvedTableScan(
      const zetasql::ResolvedTableScan* scan) final;

  // Mapping of table scans to the index hints specified on each.
  absl::flat_hash_map<const zetasql::ResolvedTableScan*, std::string>
      index_hints_map_;

  // The database schema.
  const Schema* schema_;

  // Whether to disable checks around using null-filtered indexes in SQL
  // queries.
  const bool disable_null_filtered_index_check_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INDEX_HINT_VALIDATOR_H_
