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

#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"

#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/table_valued_function.h"
#include "absl/status/status.h"
#include "absl/strings/str_split.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/column.h"

const absl::flat_hash_map<std::string, std::string> kPgToSpannerSchemaMapping =
    {{"information_schema", "pg_information_schema"}};

const std::vector<std::string> kUpperCaseSystemSchemaList = {"SPANNER_SYS"};

namespace postgres_translator {
namespace spangres {

// GetCatalogPathForTable returns the full catalog path for a table. It is safe
// to use because Spanner does not allow periods inside of names.
const std::vector<std::string> SpangresUserCatalog::GetCatalogPathForTable(
    const zetasql::Table* table) const {
  return absl::StrSplit(table->FullName(), '.');
}

absl::StatusOr<std::vector<absl::string_view>>
SpangresUserCatalog::GetPrimaryKeyColumns(const zetasql::Table& table) const {
  std::vector<absl::string_view> key_columns;
    if (table.Is<google::spanner::emulator::backend::QueryableTable>()) {
    const google::spanner::emulator::backend::QueryableTable* queryable_table =
        table.GetAs<google::spanner::emulator::backend::QueryableTable>();
    if (queryable_table == nullptr) {
      return absl::InternalError(
          absl::StrCat("Table ", table.FullName(), " is not found."));
    }
    absl::Span<const google::spanner::emulator::backend::KeyColumn* const>
        primary_key = queryable_table->wrapped_table()->primary_key();
    key_columns.reserve(primary_key.size());
    for (const auto& key : primary_key) {
      key_columns.push_back(key->column()->Name());
    }
  }
  return key_columns;
}

}  // namespace spangres
}  // namespace postgres_translator
