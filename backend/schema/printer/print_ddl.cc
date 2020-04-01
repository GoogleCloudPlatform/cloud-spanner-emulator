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

#include "backend/schema/printer/print_ddl.h"

#include <memory>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/datamodel/types.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

const CaseInsensitiveStringSet* SchemaReservedWords() {
  static const CaseInsensitiveStringSet* const reserved_words =
      new CaseInsensitiveStringSet{
          // clang-format off
"ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT",
"BETWEEN", "BY", "CASE", "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS",
"CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END",
"ENUM", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH",
"FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS", "HASH",
"HAVING", "IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL", "INTO",
"IS", "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL",
"NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER", "OVER",
"PARTITION", "PRECEDING", "PROTO", "RANGE", "RECURSIVE", "RESPECT", "RIGHT",
"ROLLUP", "ROWS", "SELECT", "SET", "SOME", "STRUCT", "TABLESAMPLE", "THEN",
"TO", "TREAT", "TRUE", "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE",
"WINDOW", "WITH", "WITHIN",
          // clang-format on
      };
  return reserved_words;
}

// Check to see if a variable name matches a KeyWord and enclose in backticks if
// that is true.
bool IsReservedWord(const std::string& name) {
  return SchemaReservedWords()->contains(name);
}

std::string PrintQualifiedName(const std::string& name) {
  return absl::Substitute("`$0`", name);
}

std::string PrintName(const std::string& name) {
  return IsReservedWord(name) ? PrintQualifiedName(name) : name;
}

// Converts a column type and it's length (if applicable) to the corresponding
// DDL type string.
std::string TypeToString(const zetasql::Type* type,
                         absl::optional<int64_t> max_length) {
  std::string type_name = type->TypeName(zetasql::PRODUCT_EXTERNAL);
  if (type->IsString() || type->IsBytes()) {
    absl::StrAppend(
        &type_name, "(",
        (max_length.has_value() ? absl::StrCat(max_length.value()) : "MAX"),
        ")");
  }
  return type_name;
}

}  // namespace

std::string OnDeleteActionToString(Table::OnDeleteAction action) {
  switch (action) {
    case Table::OnDeleteAction::kNoAction:
      return "NO ACTION";
    case Table::OnDeleteAction::kCascade:
      return "CASCADE";
  }
}

std::string ColumnTypeToString(const zetasql::Type* type,
                               absl::optional<int64_t> max_length) {
  if (type->IsArray()) {
    return "ARRAY<" +
           TypeToString(type->AsArray()->element_type(), max_length) + ">";
  } else {
    return TypeToString(type, max_length);
  }
}

std::string PrintColumn(const Column* column) {
  std::string ddl_string = PrintName(column->Name());
  absl::StrAppend(
      &ddl_string, " ",
      ColumnTypeToString(column->GetType(), column->declared_max_length()));
  if (!column->is_nullable()) {
    absl::StrAppend(&ddl_string, " NOT NULL");
  }
  if (column->GetType()->IsTimestamp() &&
      column->has_allows_commit_timestamp()) {
    absl::StrAppend(&ddl_string, " OPTIONS (\n    allow_commit_timestamp = ",
                    column->allows_commit_timestamp() ? "true" : "false",
                    "\n  )");
  }
  return ddl_string;
}

std::string PrintKeyColumn(const KeyColumn* column) {
  return absl::Substitute("$0$1", PrintName(column->column()->Name()),
                          (column->is_descending() ? " DESC" : ""));
}

std::string PrintIndex(const Index* index) {
  std::string ddl_string;
  absl::StrAppend(&ddl_string, "CREATE", (index->is_unique() ? " UNIQUE" : ""),
                  (index->is_null_filtered() ? " NULL_FILTERED" : ""),
                  " INDEX ", PrintName(index->Name()), " ON ",
                  PrintName(index->indexed_table()->Name()), "(");

  std::vector<std::string> pk_clause;
  pk_clause.reserve(index->key_columns().size());
  for (int i = 0; i < index->key_columns().size(); ++i) {
    pk_clause.push_back(PrintKeyColumn(index->key_columns()[i]));
  }
  absl::StrAppend(&ddl_string, absl::StrJoin(pk_clause, ", "), ")");

  if (!index->stored_columns().empty()) {
    absl::StrAppend(&ddl_string, " STORING (");
    std::vector<std::string> storing_clause;
    storing_clause.reserve(index->stored_columns().size());
    for (int i = 0; i < index->stored_columns().size(); ++i) {
      storing_clause.push_back(PrintName(index->stored_columns()[i]->Name()));
    }
    absl::StrAppend(&ddl_string, absl::StrJoin(storing_clause, ", "), ")");
  }

  if (index->parent()) {
    absl::StrAppend(&ddl_string, ", INTERLEAVE IN ", index->parent()->Name());
  }
  return ddl_string;
}

std::string PrintTable(const Table* table) {
  std::string table_string =
      absl::Substitute("CREATE TABLE $0 (\n", PrintName(table->Name()));
  for (int i = 0; i < table->columns().size(); ++i) {
    absl::StrAppend(&table_string, "  ", PrintColumn(table->columns()[i]),
                    ",\n");
  }
  absl::StrAppend(&table_string, ") PRIMARY KEY(");

  std::vector<std::string> pk_clause;
  pk_clause.reserve(table->primary_key().size());
  for (int i = 0; i < table->primary_key().size(); ++i) {
    pk_clause.push_back(PrintKeyColumn(table->primary_key()[i]));
  }
  absl::StrAppend(&table_string, absl::StrJoin(pk_clause, ", "));

  if (table->parent() != nullptr) {
    absl::StrAppend(&table_string, "),\n");
    absl::StrAppend(&table_string, "  INTERLEAVE IN PARENT ",
                    PrintName(table->parent()->Name()), " ON DELETE ",
                    OnDeleteActionToString(table->on_delete_action()));
  } else {
    absl::StrAppend(&table_string, ")");
  }
  return table_string;
}

std::vector<std::string> PrintDDLStatements(const Schema* schema) {
  std::vector<std::string> statements;
  // Print tables
  for (auto table : schema->tables()) {
    statements.push_back(PrintTable(table));
    // Print indexes (sorted by name).
    std::vector<const Index*> indexes{table->indexes().begin(),
                                      table->indexes().end()};
    std::sort(indexes.begin(), indexes.end(),
              [](const Index* i1, const Index* i2) {
                return i1->Name() < i2->Name();
              });
    for (auto index : indexes) {
      statements.push_back(PrintIndex(index));
    }
  }
  return statements;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
