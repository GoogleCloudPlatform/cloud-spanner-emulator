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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/strings.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/parser/ddl_reserved_words.h"
#include "third_party/spanner_pg/ddl/spangres_direct_schema_printer_impl.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/base/status_macros.h"

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
  if (type->IsStruct()) {
    std::vector<std::string> fields;
    for (const zetasql::StructField& field : type->AsStruct()->fields()) {
      fields.push_back(absl::StrCat(
          field.name, " ", ColumnTypeToString(field.type, std::nullopt)));
    }
    return absl::StrCat("STRUCT<", absl::StrJoin(fields, ", "), ">");
  } else if (type->IsArray()) {
    return "ARRAY<" +
           ColumnTypeToString(type->AsArray()->element_type(), max_length) +
           ">";
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

std::string PrintOptions(::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
  return absl::StrJoin(
      options, ", ", [](std::string* out, const ddl::SetOption& option) {
        absl::StrAppend(out, option.option_name(), " = ");
        if (option.has_null_value()) {
          absl::StrAppend(out, "NULL");
        } else if (option.has_bool_value()) {
          absl::StrAppend(out, option.bool_value() ? "true" : "false");
        } else if (option.has_int64_value()) {
          absl::StrAppend(out, option.int64_value());
        } else if (option.has_double_value()) {
          absl::StrAppend(out, option.double_value());
        } else if (option.has_string_value()) {
          absl::StrAppend(out, zetasql::ToSingleQuotedStringLiteral(
                                   option.string_value()));
        } else if (!option.string_list_value().empty()) {
          absl::StrAppend(
              out, "[",
              absl::StrJoin(
                  option.string_list_value(), ", ",
                  [](std::string* out, const std::string& option) {
                    absl::StrAppend(
                        out, zetasql::ToSingleQuotedStringLiteral(option));
                  }),
              "]");

        } else {
        }
      });
}

std::string PrintChangeStream(const ChangeStream* change_stream) {
  std::string change_stream_string = absl::Substitute(
      "CREATE CHANGE STREAM $0", PrintName(change_stream->Name()));
  if (change_stream->for_clause() != nullptr) {
    absl::StrAppend(&change_stream_string, " ", "FOR ");
    const ddl::ChangeStreamForClause* for_clause = change_stream->for_clause();
    if (for_clause->has_all()) {
      absl::StrAppend(&change_stream_string, "ALL");
    } else if (for_clause->has_tracked_tables()) {
      absl::StrAppend(
          &change_stream_string,
          absl::StrJoin(
              for_clause->tracked_tables().table_entry(), ", ",
              [](std::string* out,
                 ddl::ChangeStreamForClause_TrackedTables_Entry entry) {
                absl::StrAppend(out, PrintName(entry.table_name()));
                if (entry.has_tracked_columns()) {
                  absl::StrAppend(
                      out, "(",
                      absl::StrJoin(
                          entry.tracked_columns().column_name(), ", ",
                          [](std::string* out, std::string column_name) {
                            absl::StrAppend(out, PrintName(column_name));
                          }),
                      ")");
                }
              }));
    }
  }
  // Options with null values shouldn't be printed out.
  if (change_stream->value_capture_type().has_value() ||
      change_stream->retention_period().has_value()) {
    absl::StrAppend(&change_stream_string, " ", "OPTIONS ( ",
                    PrintOptions(change_stream->options()), " )");
  }

  return change_stream_string;
}

std::string PrintModelColumnOptions(const Model::ModelColumn& model_column) {
  std::vector<std::string> options;
  if (model_column.is_required.has_value()) {
    options.push_back(absl::StrCat(
        "required = ", *model_column.is_required ? "true" : "false"));
  }
  std::string options_string;
  if (!options.empty()) {
    options_string =
        absl::StrCat(" OPTIONS ( ", absl::StrJoin(options, ", "), " )");
  }
  return options_string;
}

std::string PrintModelColumn(const Model::ModelColumn& model_column) {
  return absl::StrCat(model_column.name, " ",
                      ColumnTypeToString(model_column.type, std::nullopt),
                      PrintModelColumnOptions(model_column));
}

std::string PrintModelOptions(const Model* model) {
  std::vector<std::string> options;
  if (model->endpoint().has_value()) {
    options.push_back(absl::StrCat(
        "endpoint = ",
        zetasql::ToSingleQuotedStringLiteral(*model->endpoint())));
  }

  if (!model->endpoints().empty()) {
    options.push_back(absl::StrCat(
        "endpoints = [ ",
        absl::StrJoin(model->endpoints(), ", ",
                      [](std::string* out, const std::string& endpoint) {
                        absl::StrAppend(
                            out,
                            zetasql::ToSingleQuotedStringLiteral(endpoint));
                      }),
        " ]"));
  }

  if (model->default_batch_size().has_value()) {
    options.push_back(
        absl::StrCat("default_batch_size = ", *model->default_batch_size()));
  }

  std::string options_string;
  if (!options.empty()) {
    options_string =
        absl::StrCat(" OPTIONS ( ", absl::StrJoin(options, ", "), " )");
  }
  return options_string;
}

std::string PrintModel(const Model* model) {
  std::string statement = absl::Substitute("CREATE MODEL $0\n", model->Name());
  absl::StrAppend(&statement, "INPUT(\n");
  for (const Model::ModelColumn& model_column : model->input()) {
    absl::StrAppend(&statement, "  ", PrintModelColumn(model_column), ",\n");
  }
  absl::StrAppend(&statement, ")\n");
  absl::StrAppend(&statement, "OUTPUT(\n");
  for (const Model::ModelColumn& model_column : model->output()) {
    absl::StrAppend(&statement, "  ", PrintModelColumn(model_column), ",\n");
  }
  absl::StrAppend(&statement, ")\n");

  if (model->is_remote()) {
    absl::StrAppend(&statement, "REMOTE\n");
  }

  absl::StrAppend(&statement, PrintModelOptions(model));
  return statement;
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
  if (!table->synonym().empty()) {
    absl::StrAppend(&table_string, "  SYNONYM(", PrintName(table->synonym()),
                    "),\n");
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
  if (foreign_key->on_delete_action() !=
      ForeignKey::Action::kActionUnspecified) {
    absl::StrAppend(&out, " ", kDeleteAction, " ",
                    ForeignKey::ActionName(foreign_key->on_delete_action()));
  }
  return out;
}

std::string PrintSequence(const Sequence* sequence) {
  std::string sequence_string = absl::Substitute(
      "CREATE SEQUENCE $0 OPTIONS (\n", PrintName(sequence->Name()));

  absl::StrAppend(&sequence_string,
                  "  sequence_kind = 'bit_reversed_positive'");
  if (sequence->skip_range_min().has_value() &&
      sequence->skip_range_max().has_value()) {
    absl::StrAppend(&sequence_string,
                    ",\n"
                    "  skip_range_min = ",
                    sequence->skip_range_min().value());
    absl::StrAppend(&sequence_string,
                    ",\n"
                    "  skip_range_max = ",
                    sequence->skip_range_max().value());
  }

  if (sequence->start_with_counter().has_value()) {
    absl::StrAppend(&sequence_string,
                    ",\n"
                    "  start_with_counter = ",
                    sequence->start_with_counter().value());
  }
  absl::StrAppend(&sequence_string, " )");

  return sequence_string;
}

std::string PrintNamedSchema(const NamedSchema* named_schema) {
  return absl::Substitute("CREATE SCHEMA $0", PrintName(named_schema->Name()));
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

  // Print sequences
  for (auto sequence : schema->sequences()) {
    statements.push_back(PrintSequence(sequence));
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
  for (auto change_stream : schema->change_streams()) {
    statements.push_back(PrintChangeStream(change_stream));
  }

  // Print models
  for (auto model : schema->models()) {
    statements.push_back(PrintModel(model));
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
