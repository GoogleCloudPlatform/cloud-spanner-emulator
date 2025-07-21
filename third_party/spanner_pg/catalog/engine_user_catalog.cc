//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/engine_user_catalog.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
absl::StatusOr<std::vector<std::string>> EngineUserCatalog::AdjustPath(
    const absl::Span<const std::string> path) {
  ZETASQL_RET_CHECK_LE(path.size(), 3);
  if (path.size() == 2) {  // path = {schema_name, table_name}
    ZETASQL_ASSIGN_OR_RETURN(const std::string& schema_name,
                     MapSchemaName(path[0], path));
    if (schema_name == kPublicSchema) {
      return std::vector<std::string>{path[1]};
    }
    return std::vector<std::string>{schema_name, path[1]};
  }
  if (path.size() == 3) {  // path = {catalog_name, schema_name, table_name}
    return absl::InvalidArgumentError(
        absl::StrCat("cross-database references are not implemented: \"",
                     path[0], ".", path[1], ".", path[2], "\""));
  }
  // path = {table_name}
  return std::vector<std::string>{path.begin(), path.end()};
}

std::string EngineUserCatalog::FullName() const {
  return engine_provided_catalog_->FullName();
}

// TODO: Implementing case-sensitive catalog lookup for objects
// in ZetaSQL's catalog.
// This would hide GSQL INFORMATION_SCHEMA from external users, so should be
// done after releasing the information_schema -> pg_information_schema mapping.
absl::Status EngineUserCatalog::FindTable(
    const absl::Span<const std::string>& path, const zetasql::Table** table,
    const FindOptions& options) {
  ZETASQL_ASSIGN_OR_RETURN(const auto& adjusted_path, AdjustPath(path));
  return engine_provided_catalog_->FindTable(adjusted_path, table, options);
}

absl::Status EngineUserCatalog::FindFunction(
    const absl::Span<const std::string>& path,
    const zetasql::Function** function, const FindOptions& options) {
  ZETASQL_ASSIGN_OR_RETURN(const auto& adjusted_path, AdjustPath(path));
  const zetasql::Function* udf_candidate;
  ZETASQL_RETURN_IF_ERROR(engine_provided_catalog_->FindFunction(
      adjusted_path, &udf_candidate, options));
  *function = udf_candidate;
  return absl::OkStatus();
}

absl::Status EngineUserCatalog::FindTableValuedFunction(
    const absl::Span<const std::string>& path,
    const zetasql::TableValuedFunction** tvf, const FindOptions& options) {
  const zetasql::TableValuedFunction* tvf_candidate;
  ZETASQL_RETURN_IF_ERROR(engine_provided_catalog_->FindTableValuedFunction(
      path, &tvf_candidate, options));
  *tvf = tvf_candidate;
  return absl::OkStatus();
}

absl::StatusOr<TableName> EngineUserCatalog::GetTableNameForGsqlTable(
    const zetasql::Table& table) const {
  const std::vector<std::string> name_path = GetCatalogPathForTable(&table);
  ZETASQL_ASSIGN_OR_RETURN(bool is_upper_case_path, InUppercaseCatalogPath(&table));
  ZETASQL_RET_CHECK_LT(name_path.size(), 3);
  if (name_path.size() == 2) {
    absl::string_view schema_name = name_path[0];
    auto iter = engine_to_pg_schema_.find(schema_name);
    if (iter != engine_to_pg_schema_.end()) {
      schema_name = iter->second;
    }
    auto lowered_if_upper = is_upper_case_path
        ? absl::AsciiStrToLower(schema_name) : std::string(schema_name);

    // If MapSchemaName returns an error, then the given zetasql::Table is in
    // a blocked schema or a schema hidden by spangres translation.
    auto mapped_back = MapSchemaName(lowered_if_upper, name_path);
    ZETASQL_RET_CHECK_OK(mapped_back.status())
            << "Attempting to get postgres name from table in hidden or blocked"
               " schemas: " << table.FullName();

    return is_upper_case_path ?
        TableName({lowered_if_upper, absl::AsciiStrToLower(name_path[1])}) :
        TableName({lowered_if_upper, name_path[1]});
  } else {
    ZETASQL_RET_CHECK(!is_upper_case_path)
        << "Table at the top level shouldn't be InUpperCaseCatalogPath "
        << table.FullName();
    return TableName({name_path[0]});
  }
}

absl::StatusOr<std::string> EngineUserCatalog::MapSchemaName(
    std::string schema_name, const absl::Span<const std::string>& path) const {
  if (blocked_schemas_.contains(schema_name)) {
    return zetasql::Catalog::ObjectNotFoundError<zetasql::Table>(path);
  }
  auto iter = pg_to_engine_schema_.find(schema_name);
  if (iter != pg_to_engine_schema_.end()) {
    return iter->second;
  }
  // This check ensures we never return an underlying schema that should have
  // been hidden by pg_to_engine_schema_ mappings e.g., without this logic,
  // quoted upper/mixed case INFORMATION_SCHEMA identifier will return the
  // underlying GSQL INFORMATION_SCHEMA which should be hidden from users.
  if (pg_to_engine_schema_.contains(absl::AsciiStrToLower(schema_name))) {
    return zetasql::Catalog::ObjectNotFoundError<zetasql::Table>(path);
  }
  return schema_name;
}

absl::StatusOr<bool> EngineUserCatalog::InUppercaseCatalogPath(
    const zetasql::Table* gsql_table) const {
  const std::vector<std::string> name_span = GetCatalogPathForTable(gsql_table);
  // A ZetaSQL path should not be greater than three. The check protects this
  // function in case this guideline is changed in the future.
  ZETASQL_RET_CHECK_LE(name_span.size(), 3);

  std::string schema_name;
  if (name_span.size() == 2) {
    schema_name = name_span[0];
  }
  if (name_span.size() == 3) {
    schema_name = name_span[1];
  }
  if (!schema_name.empty()) {
    for (const std::string& listed_schema : uppercase_schema_list_) {
      if (schema_name == listed_schema) return true;
    }
  }
  // If the name_span size is one, there is no schema so return false.
  return false;
}
}  // namespace postgres_translator
