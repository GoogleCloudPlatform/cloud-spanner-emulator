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

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/parser/ddl_reserved_words.h"
#include "third_party/spanner_pg/ddl/spangres_direct_schema_printer_impl.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using ::postgres_translator::spangres::SpangresSchemaPrinter;

namespace {

std::string PrintQualifiedName(const std::string& name) {
  return absl::Substitute("`$0`", name);
}

std::string PrintName(const std::string& name) {
  return ddl::IsReservedWord(name) ? PrintQualifiedName(name) : name;
}

std::string PrintColumnNameList(absl::Span<const Column* const> columns) {
  return absl::StrJoin(columns, ", ",
                       [](std::string* out, const Column* column) {
                         absl::StrAppend(out, PrintName(column->Name()));
                       });
}

// Converts a column type and it's length (if applicable) to the corresponding
// DDL type string.
std::string TypeToString(const zetasql::Type* type,
                         std::optional<int64_t> max_length) {
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

std::string RowDeletionPolicyToString(const ddl::RowDeletionPolicy& policy) {
  std::string str;
  absl::StrAppend(&str, "OLDER_THAN(");
  absl::StrAppend(&str, policy.column_name());
  absl::StrAppend(&str, ", ");
  absl::StrAppend(&str, "INTERVAL ");
  absl::StrAppend(&str, policy.older_than().count());
  absl::StrAppend(&str, " DAY)");
  return str;
}

std::string ColumnTypeToString(const zetasql::Type* type,
                               std::optional<int64_t> max_length) {
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
  if (column->is_generated()) {
    absl::StrAppend(
        &ddl_string,
        absl::Substitute(" AS $0 STORED", column->expression().value()));
  } else if (column->has_default_value()) {
    absl::StrAppend(
        &ddl_string,
        absl::Substitute(" DEFAULT ($0)", column->expression().value()));
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

std::string PrintView(const View* view) {
  std::string view_string =
      absl::Substitute("CREATE VIEW $0", PrintName(view->Name()));
  if (view->security() == View::SqlSecurity::INVOKER) {
    absl::StrAppend(&view_string, " SQL SECURITY INVOKER");
  }
  absl::StrAppend(&view_string, " AS ", view->body());
  return view_string;
}

std::string PrintTable(const Table* table) {
  std::string table_string =
      absl::Substitute("CREATE TABLE $0 (\n", PrintName(table->Name()));
  for (const Column* column : table->columns()) {
    absl::StrAppend(&table_string, "  ", PrintColumn(column), ",\n");
  }
  for (const ForeignKey* foreign_key : table->foreign_keys()) {
    absl::StrAppend(&table_string, "  ", PrintForeignKey(foreign_key), ",\n");
  }
  for (const CheckConstraint* check_constraint : table->check_constraints()) {
    absl::StrAppend(&table_string, "  ", PrintCheckConstraint(check_constraint),
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

  if (table->row_deletion_policy().has_value()) {
    absl::StrAppend(&table_string, ", ROW DELETION POLICY (");
    absl::StrAppend(&table_string, RowDeletionPolicyToString(
                                       table->row_deletion_policy().value()));
    absl::StrAppend(&table_string, ")");
  }
  return table_string;
}

void TopologicalOrderViews(const View* view,
                           absl::flat_hash_set<const SchemaNode*>* visited,
                           std::vector<const View*>* views) {
  if (visited->find(view) != visited->end()) {
    return;
  }
  visited->insert(view);
  for (auto dependency : view->dependencies()) {
    auto view_dep = dependency->As<const View>();
    if (view_dep == nullptr) {
      continue;
    }
    TopologicalOrderViews(view_dep, visited, views);
  }
  views->push_back(view);
}

std::string PrintCheckConstraint(const CheckConstraint* check_constraint) {
  std::string out;
  if (!check_constraint->has_generated_name()) {
    absl::StrAppend(&out, "CONSTRAINT ", PrintName(check_constraint->Name()),
                    " ");
  }
  absl::StrAppend(&out, "CHECK(", check_constraint->expression(), ")");
  return out;
}

std::string PrintForeignKey(const ForeignKey* foreign_key) {
  std::string out;
  if (!foreign_key->constraint_name().empty()) {
    absl::StrAppend(&out, "CONSTRAINT ",
                    PrintName(foreign_key->constraint_name()), " ");
  }
  absl::StrAppend(&out, "FOREIGN KEY(",
                  PrintColumnNameList(foreign_key->referencing_columns()),
                  ") REFERENCES ",
                  PrintName(foreign_key->referenced_table()->Name()), "(",
                  PrintColumnNameList(foreign_key->referenced_columns()), ")");
  return out;
}

absl::StatusOr<std::vector<std::string>> PrintDDLStatements(
    const Schema* schema) {
  std::vector<std::string> statements;
  if (schema->dialect() == database_api::DatabaseDialect::POSTGRESQL) {
    absl::StatusOr<std::unique_ptr<SpangresSchemaPrinter>> printer =
        postgres_translator::spangres::CreateSpangresDirectSchemaPrinter();
    ZETASQL_RETURN_IF_ERROR(printer.status());
    ddl::DDLStatementList ddl_statements = schema->Dump();
    for (const ddl::DDLStatement& statement : ddl_statements.statement()) {
      absl::StatusOr<std::vector<std::string>> printed_statements =
          (*printer)->PrintDDLStatementForEmulator(statement);
      ZETASQL_RETURN_IF_ERROR(printed_statements.status());
      statements.insert(statements.end(), (*printed_statements).begin(),
                        (*printed_statements).end());
    }
    return statements;
  }

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
      if (!index->is_managed()) {
        statements.push_back(PrintIndex(index));
      }
    }
  }

  // Print views that depend on the tables/indexes.
  absl::flat_hash_set<const SchemaNode*> visited;
  std::vector<const View*> views;
  for (auto view : schema->views()) {
    TopologicalOrderViews(view, &visited, &views);
  }
  for (auto view : views) {
    statements.push_back(PrintView(view));
  }
  return statements;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
