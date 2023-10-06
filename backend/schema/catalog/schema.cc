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

#include "backend/schema/catalog/schema.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_map.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/parser/ddl_parser.h"
#include "backend/schema/updater/ddl_type_conversion.h"
#include "re2/re2.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

const char kManagedIndexNonFingerprintRegex[] = "(IDX_\\w+_)[0-9A-F]{16}";
const int kFingerprintLength = 16;

const View* Schema::FindView(const std::string& view_name) const {
  auto itr = views_map_.find(view_name);
  if (itr == views_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const View* Schema::FindViewCaseSensitive(const std::string& view_name) const {
  auto view = FindView(view_name);
  if (!view || view->Name() != view_name) {
    return nullptr;
  }
  return view;
}

const Table* Schema::FindTable(const std::string& table_name) const {
  auto itr = tables_map_.find(table_name);
  if (itr == tables_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Table* Schema::FindTableCaseSensitive(
    const std::string& table_name) const {
  auto table = FindTable(table_name);
  if (!table || table->Name() != table_name) {
    return nullptr;
  }
  return table;
}

const Index* Schema::FindIndex(const std::string& index_name) const {
  auto itr = index_map_.find(index_name);
  if (itr == index_map_.end()) {
    return this->FindManagedIndex(index_name);
  }
  return itr->second;
}

const Index* Schema::FindIndexCaseSensitive(
    const std::string& index_name) const {
  auto index = FindIndex(index_name);
  if (!index || index->Name() != index_name) {
    return nullptr;
  }
  return index;
}

const ChangeStream* Schema::FindChangeStream(
    const std::string& change_stream_name) const {
  auto itr = change_streams_map_.find(change_stream_name);
  if (itr == change_streams_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Index* Schema::FindManagedIndex(const std::string& index_name) const {
  // Check that the index_name matches the format of managed index names, and
  // extract the non-fingerprint part of the index.
  std::string non_fingerprint_index_name;
  if (!RE2::FullMatch(index_name, kManagedIndexNonFingerprintRegex,
                      &non_fingerprint_index_name)) {
    return nullptr;
  }

  for (const auto& itr : index_map_) {
    if (itr.second->is_managed() &&
        itr.first.length() ==
            non_fingerprint_index_name.length() + kFingerprintLength &&
        itr.first.find(non_fingerprint_index_name) != std::string::npos) {
      return itr.second;
    }
  }
  return nullptr;
}

ddl::ForeignKey::Action FindForeignKeyOnDeleteAction(const ForeignKey* fk) {
  return fk->on_delete_action() == ForeignKey::Action::kCascade
             ? ddl::ForeignKey::CASCADE
             : ddl::ForeignKey::NO_ACTION;
}

void DumpIndex(const Index* index, ddl::CreateIndex& create_index) {
  ABSL_CHECK_NE(index, nullptr);  // Crash OK
  create_index.set_index_name(index->Name());
  create_index.set_index_base_name(index->indexed_table()->Name());
  create_index.set_unique(index->is_unique());
  if (index->parent() != nullptr) {
    create_index.set_interleave_in_table(index->parent()->Name());
  }
  for (const KeyColumn* key_column : index->key_columns()) {
    ddl::KeyPartClause* key_part_clause = create_index.add_key();
    key_part_clause->set_key_name(key_column->column()->Name());
    if (!key_column->is_descending() && key_column->is_nulls_last()) {
      key_part_clause->set_order(ddl::KeyPartClause::ASC_NULLS_LAST);
    } else if (key_column->is_descending() && !key_column->is_nulls_last()) {
      key_part_clause->set_order(ddl::KeyPartClause::DESC_NULLS_FIRST);
    } else {
      key_part_clause->set_order(key_column->is_descending()
                                     ? ddl::KeyPartClause::DESC
                                     : ddl::KeyPartClause::ASC);
    }
  }
  for (const Column* stored_column : index->stored_columns()) {
    ddl::StoredColumnDefinition* stored_column_def =
        create_index.add_stored_column_definition();
    stored_column_def->set_name(stored_column->Name());
  }
}

template <typename ColumnDef>
void SetColumnExpression(const Column* column, ColumnDef& column_def) {
  if (column->expression().has_value()) {
    column_def.set_expression(*column->expression());
    if (column->original_expression().has_value()) {
      column_def.mutable_expression_origin()->set_original_expression(
          *column->original_expression());
    }
  }
}

void DumpColumn(const Column* column, ddl::ColumnDefinition& column_def) {
  ABSL_CHECK_NE(column, nullptr);  // Crash OK
  column_def.set_column_name(column->Name());
  const zetasql::Type* column_type = column->GetType();
  if (column_type != nullptr) {
    ddl::ColumnDefinition type_column_def =
        GoogleSqlTypeToDDLColumnType(column_type);
    column_def.set_type(type_column_def.type());
    if (column_type->IsArray()) {
      *column_def.mutable_array_subtype() = type_column_def.array_subtype();
    }
  }
  if (column->declared_max_length().has_value()) {
    if (column_type->IsArray()) {
      column_def.mutable_array_subtype()->set_length(
          *column->declared_max_length());
    } else {
      column_def.set_length(*column->declared_max_length());
    }
  }
  column_def.set_not_null(!column->is_nullable());
  if (column->allows_commit_timestamp()) {
    ddl::SetOption* set_option = column_def.add_set_options();
    set_option->set_option_name(ddl::kPGCommitTimestampOptionName);
    set_option->set_bool_value(true);
  }
  if (column->has_default_value()) {
    SetColumnExpression(column, *column_def.mutable_column_default());
  }
  if (column->is_generated()) {
    ddl::ColumnDefinition_GeneratedColumnDefinition* generated_column =
        column_def.mutable_generated_column();
    // Non-stored generated columns are not supported.
    generated_column->set_stored(true);
    SetColumnExpression(column, *generated_column);
  }
}

void DumpForeignKey(const ForeignKey* foreign_key,
                    ddl::ForeignKey& foreign_key_def) {
  ABSL_CHECK_NE(foreign_key, nullptr);  // Crash OK
  foreign_key_def.set_enforced(true);
  if (!foreign_key->constraint_name().empty()) {
    // Do not set constraint name when it is a generated name.
    foreign_key_def.set_constraint_name(foreign_key->Name());
  }
  foreign_key_def.set_referenced_table_name(
      foreign_key->referenced_table()->Name());
  for (const Column* column : foreign_key->referencing_columns()) {
    foreign_key_def.add_constrained_column_name(column->Name());
  }
  for (const Column* column : foreign_key->referenced_columns()) {
    foreign_key_def.add_referenced_column_name(column->Name());
  }
  if (foreign_key->on_delete_action() !=
      ForeignKey::Action::kActionUnspecified) {
    foreign_key_def.set_on_delete(FindForeignKeyOnDeleteAction(foreign_key));
  }
}

void DumpInterleaveClause(const Table* table,
                          ddl::InterleaveClause& interleave_clause) {
  ABSL_CHECK_NE(table, nullptr);  // Crash OK
  interleave_clause.set_table_name(table->parent()->Name());
  interleave_clause.set_on_delete(table->on_delete_action() ==
                                          Table::OnDeleteAction::kCascade
                                      ? ddl::InterleaveClause::CASCADE
                                      : ddl::InterleaveClause::NO_ACTION);
}

void DumpCheckConstraint(const CheckConstraint* check_constraint,
                         ddl::CheckConstraint& check_constraint_def) {
  check_constraint_def.set_enforced(true);
  if (!check_constraint->has_generated_name()) {
    check_constraint_def.set_name(check_constraint->Name());
  }
  check_constraint_def.set_expression(check_constraint->expression());
  if (check_constraint->original_expression().has_value()) {
    check_constraint_def.mutable_expression_origin()->set_original_expression(
        *check_constraint->original_expression());
  }
}

void DumpChangeStream(const ChangeStream* change_stream,
                      ddl::CreateChangeStream& create_change_stream) {
  create_change_stream.set_change_stream_name(change_stream->Name());
  if (change_stream->for_clause() != nullptr) {
    ddl::ChangeStreamForClause* for_clause =
        create_change_stream.mutable_for_clause();
    *for_clause = *change_stream->for_clause();
  }
  if (change_stream->value_capture_type().has_value()) {
    ddl::SetOption* set_option = create_change_stream.add_set_options();
    set_option->set_option_name(ddl::kChangeStreamValueCaptureTypeOptionName);
    set_option->set_string_value(*change_stream->value_capture_type());
  }
  if (change_stream->retention_period().has_value()) {
    ddl::SetOption* set_option = create_change_stream.add_set_options();
    set_option->set_option_name(ddl::kChangeStreamRetentionPeriodOptionName);
    set_option->set_string_value(*change_stream->retention_period());
  }
}

ddl::DDLStatementList Schema::Dump() const {
  ddl::DDLStatementList ddl_statements;
  for (const Table* table : tables_) {
    ddl::CreateTable* create_table =
        ddl_statements.add_statement()->mutable_create_table();
    create_table->set_table_name(table->Name());
    for (const Column* column : table->columns()) {
      DumpColumn(column, *create_table->add_column());
    }

    for (const ForeignKey* foreign_key : table->foreign_keys()) {
      DumpForeignKey(foreign_key, *create_table->add_foreign_key());
    }

    for (const KeyColumn* key_column : table->primary_key()) {
      ddl::KeyPartClause* key_part_clause = create_table->add_primary_key();
      key_part_clause->set_key_name(key_column->column()->Name());
    }

    if (table->parent() != nullptr) {
      DumpInterleaveClause(table, *create_table->mutable_interleave_clause());
    }

    // Unnamed check constraints are printed before named ones.
    for (const CheckConstraint* check_constraint : table->check_constraints()) {
      if (check_constraint->has_generated_name()) {
        DumpCheckConstraint(check_constraint,
                            *create_table->add_check_constraint());
      }
    }
    // Named check constraints.
    for (const CheckConstraint* check_constraint : table->check_constraints()) {
      if (!check_constraint->has_generated_name()) {
        DumpCheckConstraint(check_constraint,
                            *create_table->add_check_constraint());
      }
    }

    if (table->row_deletion_policy().has_value()) {
      *create_table->mutable_row_deletion_policy() =
          *table->row_deletion_policy();
    }
  }

  for (const auto& [unused_name, index] : index_map_) {
    if (!index->is_managed()) {
      DumpIndex(index, *ddl_statements.add_statement()->mutable_create_index());
    }
  }

  for (const View* view : views_) {
    ddl::CreateFunction* create_function =
        ddl_statements.add_statement()->mutable_create_function();
    create_function->set_function_kind(ddl::Function::VIEW);
    create_function->set_function_name(view->Name());
    if (view->security() == View::INVOKER) {
      create_function->set_sql_security(ddl::Function::INVOKER);
    }
    create_function->set_sql_body(view->body());
    if (view->body_origin().has_value()) {
      create_function->mutable_sql_body_origin()->set_original_expression(
          *view->body_origin());
    }
  }

  for (const ChangeStream* change_stream : change_streams_) {
    DumpChangeStream(
        change_stream,
        *ddl_statements.add_statement()->mutable_create_change_stream());
  }

  return ddl_statements;
}

Schema::Schema(const SchemaGraph* graph
               ,
               const database_api::DatabaseDialect& dialect
               )
    : graph_(graph)
      ,
      dialect_(dialect)
{
  views_.clear();
  views_map_.clear();
  tables_.clear();
  tables_map_.clear();
  index_map_.clear();
  change_streams_.clear();
  change_streams_map_.clear();
  for (const SchemaNode* node : graph_->GetSchemaNodes()) {
    const View* view = node->As<const View>();
    if (view != nullptr) {
      views_.push_back(view);
      views_map_[view->Name()] = view;
      continue;
    }

    const Table* table = node->As<const Table>();
    if (table != nullptr && table->is_public()) {
      tables_.push_back(table);
      tables_map_[table->Name()] = table;
      continue;
    }

    const Index* index = node->As<const Index>();
    if (index != nullptr) {
      index_map_[index->Name()] = index;
      continue;
    }

    const ChangeStream* change_stream = node->As<ChangeStream>();
    if (change_stream != nullptr) {
      change_streams_.push_back(change_stream);
      change_streams_map_[change_stream->Name()] = change_stream;
      continue;
    }

    // Columns need not be stored in the schema, they are just owned by the
    // graph.
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
