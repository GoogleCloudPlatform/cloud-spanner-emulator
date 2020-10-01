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

#include "backend/query/index_hint_validator.h"

#include <algorithm>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/updater/global_schema_names.h"
#include "common/errors.h"
#include "re2/re2.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Query hints using generated names of managed indexes are special-cased. The
// emulator and production name generators use different fingerprint algorithms,
// so the generated names have different suffixes. A hint using a managed
// index's name that was generated in production never matches the name of the
// emulator's index, and vice versa. In order to permit the use of production
// queries in the emulator, names generated in production are permitted in the
// emulator. However, in order to avoid the unintentional and unsupported use of
// emulator generated names in production, names generated in the emulator are
// not allowed in query hints, neither in production nor in the emulator.

// Returns true if an index name matches the signature of a generated managed
// index name. See also GlobalSchemaNames::GenerateManagedIndexName.
bool MatchesManagedIndexName(absl::string_view table_name,
                             absl::string_view index_name) {
  return RE2::FullMatch(index_name, absl::StrCat("IDX_", table_name, "_",
                                                 "\\w+", "_", "[0-9A-Z]{16}"));
}

// Returns true if an index's name matches the name of an index created in the
// emulator with GlobalSchemaNames::GenerateManagedIndexName.
bool HasGeneratedEmulatorName(const Index* index) {
  if (!index->is_managed()) {
    return false;  // Only managed indexes have generated names.
  }
  auto columns = index->key_columns();
  std::vector<std::string> column_names;
  std::transform(
      columns.begin(), columns.end(), std::back_inserter(column_names),
      [](const KeyColumn* column) { return column->column()->Name(); });
  return index->Name() == GlobalSchemaNames::GenerateManagedIndexName(
                              index->indexed_table()->Name(), column_names,
                              index->is_null_filtered(), index->is_unique())
                              .value();
}

}  // namespace

absl::Status IndexHintValidator::VisitResolvedTableScan(
    const zetasql::ResolvedTableScan* table_scan) {
  // Visit child nodes first.
  ZETASQL_RETURN_IF_ERROR(zetasql::ResolvedASTVisitor::DefaultVisit(table_scan));

  std::vector<const zetasql::ResolvedNode*> child_nodes;
  table_scan->GetChildNodes(&child_nodes);

  for (const zetasql::ResolvedNode* child_node : child_nodes) {
    if (child_node->node_kind() == zetasql::RESOLVED_OPTION) {
      const zetasql::ResolvedOption* hint =
          child_node->GetAs<zetasql::ResolvedOption>();
      if ((absl::EqualsIgnoreCase(hint->qualifier(), "spanner") ||
           hint->qualifier().empty()) &&
          absl::EqualsIgnoreCase(hint->name(), "force_index")) {
        // We should expect only one hint per table scan as multiple hints per
        // node is not allowed and would've been rejected by the HintValidator.
        ZETASQL_RET_CHECK_EQ(hint->value()->node_kind(), zetasql::RESOLVED_LITERAL);
        const zetasql::Value& value =
            hint->value()->GetAs<zetasql::ResolvedLiteral>()->value();
        ZETASQL_RET_CHECK(value.type()->IsString());
        index_hints_map_[table_scan] = value.string_value();
        break;
      }
    }
  }

  return absl::OkStatus();
}

absl::Status IndexHintValidator::ValidateIndexesForTables() {
  for (auto [table_scan, index_name] : index_hints_map_) {
    if (absl::EqualsIgnoreCase(index_name, "_base_table")) {
      continue;
    }
    auto table = table_scan->table();
    auto query_table = table->GetAs<QueryableTable>();
    auto schema_table = query_table->wrapped_table();
    const auto* index = schema_table->FindIndex(index_name);
    // See comments above regarding special-casing of managed indexes.
    if (index == nullptr) {
      if (MatchesManagedIndexName(schema_table->Name(), index_name)) {
        return absl::OkStatus();
      }
      return error::QueryHintIndexNotFound(schema_table->Name(), index_name);
    } else if (HasGeneratedEmulatorName(index)) {
      return error::QueryHintManagedIndexNotSupported(index_name);
    }

    if (index->is_null_filtered() && !disable_null_filtered_index_check_) {
      for (const auto* key_column : index->key_columns()) {
        const auto* source_column = key_column->column()->source_column();
        // If any of the index's columns are nullable, then it is not indexing
        // the full table and therefore, without analyzing the predicates in the
        // query to determine if they filter nulls, we cannot allow the index
        // to be used.
        if (source_column->is_nullable()) {
          return error::NullFilteredIndexUnusable(index_name);
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status IndexHintValidator::VisitResolvedQueryStmt(
    const zetasql::ResolvedQueryStmt* stmt) {
  // Visit children first to collect all hints.
  ZETASQL_RETURN_IF_ERROR(zetasql::ResolvedASTVisitor::DefaultVisit(stmt));
  // Validate all index hints.
  return ValidateIndexesForTables();
}

absl::Status IndexHintValidator::VisitResolvedInsertStmt(
    const zetasql::ResolvedInsertStmt* stmt) {
  // Visit children first to collect all hints.
  ZETASQL_RETURN_IF_ERROR(zetasql::ResolvedASTVisitor::DefaultVisit(stmt));
  // The target table should not have any hints (not allowed by ZetaSQL).
  ZETASQL_RET_CHECK(!index_hints_map_.contains(stmt->table_scan()));
  return ValidateIndexesForTables();
}

absl::Status IndexHintValidator::VisitResolvedUpdateStmt(
    const zetasql::ResolvedUpdateStmt* stmt) {
  // Visit children first to collect all hints.
  ZETASQL_RETURN_IF_ERROR(zetasql::ResolvedASTVisitor::DefaultVisit(stmt));
  // The target table should not have any hints (not allowed by ZetaSQL).
  ZETASQL_RET_CHECK(!index_hints_map_.contains(stmt->table_scan()));
  return ValidateIndexesForTables();
}

absl::Status IndexHintValidator::VisitResolvedDeleteStmt(
    const zetasql::ResolvedDeleteStmt* stmt) {
  // Visit children first to collect all hints.
  ZETASQL_RETURN_IF_ERROR(zetasql::ResolvedASTVisitor::DefaultVisit(stmt));
  // The target table should not have any hints (not allowed by ZetaSQL).
  ZETASQL_RET_CHECK(!index_hints_map_.contains(stmt->table_scan()));
  return ValidateIndexesForTables();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
