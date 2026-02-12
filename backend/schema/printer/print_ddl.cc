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

#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/common/utils.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/database_options.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/placement.h"
#include "backend/schema/catalog/property_graph.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/parser/ddl_parser.h"
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
  std::string type_name = type->TypeName(zetasql::PRODUCT_EXTERNAL,
                                         /*use_external_float32=*/true);
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
  if (column->vector_length().has_value()) {
    absl::StrAppend(&ddl_string, "(vector_length=>",
                    column->vector_length().value(), ")");
  }
  if (!column->is_nullable()) {
    absl::StrAppend(&ddl_string, " NOT NULL");
  }
  if (column->is_placement_key()) {
    absl::StrAppend(&ddl_string, " PLACEMENT KEY");
  }
  if (column->is_generated()) {
    absl::StrAppend(&ddl_string,
                    absl::Substitute(" AS $0", column->expression().value()));
    if (column->is_stored()) {
      absl::StrAppend(&ddl_string, " STORED");
    }
  } else if (column->is_identity_column()) {
    // This needs to check before the default value.
    absl::StrAppend(&ddl_string, " GENERATED BY DEFAULT AS IDENTITY");
    if (!column->sequences_used().empty()) {
      const Sequence* seq =
          static_cast<const Sequence*>(column->sequences_used().at(0));
      std::vector<std::string> options;
      if (!seq->use_default_sequence_kind_option()) {
        options.push_back(seq->sequence_kind_name());
      }
      if (seq->skip_range_min().has_value()) {
        options.push_back(absl::StrCat("SKIP RANGE ", *seq->skip_range_min(),
                                       ", ", *seq->skip_range_max()));
      }
      if (seq->start_with_counter().has_value()) {
        options.push_back(
            absl::StrCat("START COUNTER WITH ", *seq->start_with_counter()));
      }
      if (!options.empty()) {
        absl::StrAppend(&ddl_string, " (", absl::StrJoin(options, " "), ")");
      }
    }
  } else if (column->has_default_value()) {
    absl::StrAppend(
        &ddl_string,
        absl::Substitute(" DEFAULT ($0)", column->expression().value()));
    // ON UPDATE is required to have an identical DEFAULT value expression.
    if (column->has_on_update()) {
      absl::StrAppend(
          &ddl_string,
          absl::Substitute(" ON UPDATE ($0)", column->expression().value()));
    }
  }
  if (column->GetType()->IsTimestamp() &&
      column->has_allows_commit_timestamp()) {
    absl::StrAppend(&ddl_string, " OPTIONS (\n    allow_commit_timestamp = ",
                    column->allows_commit_timestamp() ? "true" : "false",
                    "\n  )");
  }
  if (column->hidden()) {
    absl::StrAppend(&ddl_string, " HIDDEN");
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
                  (index->is_search_index() ? " SEARCH" : ""),
                  (index->is_vector_index() ? " VECTOR" : ""),
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

  if (index->is_search_index()) {
    if (!index->partition_by().empty()) {
      absl::StrAppend(&ddl_string, " PARTITION BY ");
      std::vector<std::string> partition_by_clause;
      partition_by_clause.reserve(index->partition_by().size());
      for (int i = 0; i < index->partition_by().size(); ++i) {
        partition_by_clause.push_back(
            PrintName(index->partition_by()[i]->Name()));
      }
      absl::StrAppend(&ddl_string, absl::StrJoin(partition_by_clause, ", "));
    }
    if (!index->order_by().empty()) {
      absl::StrAppend(&ddl_string, " ORDER BY ");
      std::vector<std::string> order_by_clause;
      order_by_clause.reserve(index->order_by().size());
      for (int i = 0; i < index->order_by().size(); ++i) {
        auto key_column = index->order_by()[i];
        order_by_clause.push_back(
            absl::StrCat(PrintName(key_column->column()->Name()),
                         // TODO: Specifying NULLS FIRST/LAST is
                         // unsupported in the emulator. Currently, users cannot
                         // specify ASC_NULLS_LAST and DESC_NULLS_FIRST.
                         key_column->is_descending() ? " DESC" : ""));
      }
      absl::StrAppend(&ddl_string, absl::StrJoin(order_by_clause, ", "));
    }
    ddl::SearchIndexOptionsProto options = index->search_index_options();
    std::vector<std::string> options_str;
    if (options.has_sort_order_sharding()) {
      options_str.push_back(
          absl::StrCat("sort_order_sharding = ",
                       options.sort_order_sharding() ? "true" : "false"));
    }
    if (options.has_disable_automatic_uid_column()) {
      options_str.push_back(absl::StrCat(
          "disable_automatic_uid_column = ",
          options.disable_automatic_uid_column() ? "true" : "false"));
    }
    if (!options_str.empty()) {
      absl::StrAppend(&ddl_string, " OPTIONS (",
                      absl::StrJoin(options_str, ", "), ")");
    }
  }

  if (index->is_vector_index()) {
    if (!index->null_filtered_columns().empty()) {
      absl::StrAppend(&ddl_string, " WHERE ",
                      absl::StrJoin(index->null_filtered_columns(), " AND ",
                                    [](std::string* out, const Column* column) {
                                      absl::StrAppend(
                                          out, PrintName(column->Name()));
                                      absl::StrAppend(out, " IS NOT NULL");
                                    }));
    }
    ddl::VectorIndexOptionsProto options = index->vector_index_options();
    std::vector<std::string> options_str;
    if (options.has_tree_depth()) {
      options_str.push_back(
          absl::StrCat("tree_depth = ", options.tree_depth()));
    }
    if (options.has_num_leaves()) {
      options_str.push_back(
          absl::StrCat("num_leaves = ", options.num_leaves()));
    }
    if (options.has_num_branches()) {
      options_str.push_back(
          absl::StrCat("num_branches = ", options.num_branches()));
    }
    if (options.has_distance_type()) {
      options_str.push_back(absl::StrCat(
          "distance_type = ",
          zetasql::ToSingleQuotedStringLiteral(options.distance_type())));
    }
    if (options.has_leaf_scatter_factor()) {
      options_str.push_back(absl::StrCat("leaf_scatter_factor = ",
                                         options.leaf_scatter_factor()));
    }
    if (options.has_min_branch_splits()) {
      options_str.push_back(
          absl::StrCat("min_branch_splits = ", options.min_branch_splits()));
    }
    if (options.has_min_leaf_splits()) {
      options_str.push_back(
          absl::StrCat("min_leaf_splits = ", options.min_leaf_splits()));
    }
    if (options.has_locality_group()) {
      options_str.push_back(absl::StrCat(
          "locality_group = ",
          zetasql::ToSingleQuotedStringLiteral(options.locality_group())));
    }
    if (!options_str.empty()) {
      absl::StrAppend(&ddl_string, " OPTIONS ( ",
                      absl::StrJoin(options_str, ", "), " )");
    }
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
  if (change_stream->HasExplicitValidOptions()) {
    absl::StrAppend(&change_stream_string, " ", "OPTIONS ( ",
                    PrintOptions(change_stream->options()), " )");
  }

  return change_stream_string;
}

std::string PrintPlacement(const Placement* placement) {
  std::string placement_string = absl::Substitute(
      "CREATE PLACEMENT $0", PrintName(placement->PlacementName()));

  // Options with null values shouldn't be printed out.
  if (placement->HasExplicitValidOptions()) {
    absl::StrAppend(&placement_string, " ", "OPTIONS ( ",
                    PrintOptions(placement->options()), " )");
  }

  return placement_string;
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

std::string PrintLabel(const PropertyGraph::Label& label) {
  std::string result = label.name;
  if (!label.property_names.empty()) {
    absl::StrAppend(&result, " PROPERTIES (",
                    absl::StrJoin(label.property_names, ", "), ")");
  }
  return result;
}

std::string PrintGraphElementTable(
    const PropertyGraph* property_graph,
    const PropertyGraph::GraphElementTable& graph_element_table) {
  std::string statement = absl::Substitute("$0", graph_element_table.name());
  if (!graph_element_table.key_clause_columns().empty()) {
    absl::StrAppend(
        &statement, " KEY(",
        absl::StrJoin(graph_element_table.key_clause_columns(), ", "), ")");
  }
  // Print labels and their properties
  for (absl::string_view label_name : graph_element_table.label_names()) {
    const PropertyGraph::Label* label;
    absl::Status find_status =
        property_graph->FindLabelByName(label_name, label);
    ABSL_CHECK_OK(find_status);
    absl::StrAppend(&statement, " LABEL ", PrintLabel(*label), "\n");
  }
  if (graph_element_table.element_kind() ==
      PropertyGraph::GraphElementKind::EDGE) {
    absl::StrAppend(
        &statement, " SOURCE KEY(",
        absl::StrJoin(
            graph_element_table.source_node_reference().edge_table_column_names,
            ", "),
        ") REFERENCES ",
        graph_element_table.source_node_reference().node_table_name, "(",
        absl::StrJoin(
            graph_element_table.source_node_reference().node_table_column_names,
            ", "),
        ")");
    absl::StrAppend(
        &statement, " DESTINATION KEY(",
        absl::StrJoin(
            graph_element_table.target_node_reference().edge_table_column_names,
            ", "),
        ") REFERENCES ",
        graph_element_table.target_node_reference().node_table_name, "(",
        absl::StrJoin(
            graph_element_table.target_node_reference().node_table_column_names,
            ", "),
        ")");
  }
  return statement;
}

std::string PrintPropertyGraph(const PropertyGraph* property_graph) {
  std::string statement =
      absl::Substitute("CREATE PROPERTY GRAPH $0\n", property_graph->Name());

  absl::StrAppend(&statement, "NODE TABLES(\n");
  for (const PropertyGraph::GraphElementTable& node_table :
       property_graph->NodeTables()) {
    absl::StrAppend(&statement, "  ",
                    PrintGraphElementTable(property_graph, node_table), ",\n");
  }
  absl::StrAppend(&statement, ")\n");

  absl::StrAppend(&statement, "EDGE TABLES(\n");
  for (const PropertyGraph::GraphElementTable& edge_table :
       property_graph->EdgeTables()) {
    absl::StrAppend(&statement, "  ",
                    PrintGraphElementTable(property_graph, edge_table), ",\n");
  }
  absl::StrAppend(&statement, ")\n");

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
    if (table->interleave_type().value() == Table::InterleaveType::kInParent) {
      absl::StrAppend(&table_string, "  INTERLEAVE IN PARENT ",
                      PrintName(table->parent()->Name()), " ON DELETE ",
                      OnDeleteActionToString(table->on_delete_action()));
    } else {
      absl::StrAppend(&table_string, "  INTERLEAVE IN ",
                      PrintName(table->parent()->Name()));
    }
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

// Moves the nodes with no dependencies to the front of the list.
void TopologicalOrderSchemaNodes(
    const SchemaNode* node, absl::flat_hash_set<const SchemaNode*>* visited,
    std::vector<std::string>* statements) {
  if (visited->find(node) != visited->end()) {
    return;
  }
  visited->insert(node);

  if (const View* view = node->As<const View>(); view != nullptr) {
    for (auto dependency : view->dependencies()) {
      TopologicalOrderSchemaNodes(dependency, visited, statements);
    }
    statements->push_back(PrintView(view));
  }

  if (const Table* table = node->As<const Table>(); table != nullptr) {
    for (const Column* column : table->columns()) {
      for (const Column* column_dep : column->dependent_columns()) {
        TopologicalOrderSchemaNodes(column_dep->table(), visited, statements);
      }
      for (const SchemaNode* sequence_dep : column->sequences_used()) {
        TopologicalOrderSchemaNodes(sequence_dep, visited, statements);
      }
      for (const SchemaNode* udf_dep : column->udf_dependencies()) {
        TopologicalOrderSchemaNodes(udf_dep, visited, statements);
      }
      // Omitting indexes since their dependencies to UDFs are through columns.
    }
    for (const CheckConstraint* check_constraint : table->check_constraints()) {
      for (const SchemaNode* udf_dep : check_constraint->udf_dependencies()) {
        TopologicalOrderSchemaNodes(udf_dep, visited, statements);
      }
    }

    statements->push_back(PrintTable(table));
    std::vector<const Index*> indexes{table->indexes().begin(),
                                      table->indexes().end()};
    std::sort(indexes.begin(), indexes.end(),
              [](const Index* i1, const Index* i2) {
                return i1->Name() < i2->Name();
              });
    for (const Index* index : indexes) {
      if (!index->is_managed()) {
        statements->push_back(PrintIndex(index));
      }
    }
  }

  if (const Udf* udf = node->As<const Udf>(); udf != nullptr) {
    for (const SchemaNode* udf_dep : udf->dependencies()) {
      TopologicalOrderSchemaNodes(udf_dep, visited, statements);
    }

    statements->push_back(PrintUdf(udf));
  }

  if (const Sequence* sequence = node->As<const Sequence>();
      sequence != nullptr) {
    // Not an internal sequence, e.g., used by an identity column.
    if (!absl::StartsWith(sequence->Name(), "_")) {
      statements->push_back(PrintSequence(sequence));
    }
  }

  if (const ChangeStream* change_stream = node->As<ChangeStream>();
      change_stream != nullptr) {
    statements->push_back(PrintChangeStream(change_stream));
  }

  if (const Placement* placement = node->As<Placement>();
      placement != nullptr) {
    statements->push_back(PrintPlacement(placement));
  }

  if (const Model* model = node->As<const Model>(); model != nullptr) {
    statements->push_back(PrintModel(model));
  }

  if (const PropertyGraph* property_graph = node->As<const PropertyGraph>();
      property_graph != nullptr) {
    statements->push_back(PrintPropertyGraph(property_graph));
  }

  if (const NamedSchema* named_schema = node->As<const NamedSchema>();
      named_schema != nullptr) {
    statements->push_back(PrintNamedSchema(named_schema));
  }
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
  if (!foreign_key->enforced()) {
    absl::StrAppend(&out, " NOT ENFORCED");
  }
  return out;
}

std::string PrintSequence(const Sequence* sequence) {
  std::string sequence_string =
      absl::Substitute("CREATE SEQUENCE $0", PrintName(sequence->Name()));

  if (sequence->created_from_options()) {
    absl::StrAppend(&sequence_string, " OPTIONS (\n");
    std::vector<std::string> options;
    if (!sequence->use_default_sequence_kind_option()) {
      options.push_back("  sequence_kind = 'bit_reversed_positive'");
    }
    if (sequence->skip_range_min().has_value() &&
        sequence->skip_range_max().has_value()) {
      options.push_back(absl::StrCat("  skip_range_min = ",
                                     sequence->skip_range_min().value()));
      options.push_back(absl::StrCat("  skip_range_max = ",
                                     sequence->skip_range_max().value()));
    }

    if (sequence->start_with_counter().has_value()) {
      options.push_back(absl::StrCat("  start_with_counter = ",
                                     sequence->start_with_counter().value()));
    }
    absl::StrAppend(&sequence_string, absl::StrJoin(options, ",\n"), ")");
  } else if (sequence->created_from_syntax()) {
    if (!sequence->use_default_sequence_kind_option()) {
      absl::StrAppend(&sequence_string, " BIT_REVERSED_POSITIVE");
    }
    if (sequence->skip_range_min().has_value() &&
        sequence->skip_range_max().has_value()) {
      absl::StrAppend(&sequence_string, " SKIP RANGE ",
                      sequence->skip_range_min().value(), ", ",
                      sequence->skip_range_max().value());
    }
    if (sequence->start_with_counter().has_value()) {
      absl::StrAppend(&sequence_string, " START COUNTER WITH ",
                      sequence->start_with_counter().value());
    }
  }

  return sequence_string;
}

std::string PrintUdf(const Udf* udf) {
  if (udf->body_origin().has_value()) {
    return udf->body_origin().value();
  }

  std::string udf_string =
      absl::Substitute("CREATE FUNCTION $0", PrintName(udf->Name()));

  const zetasql::FunctionSignature* signature = udf->signature();
  if (signature) {
    const std::vector<zetasql::FunctionArgumentType>& arguments =
        signature->arguments();
    std::vector<std::string> arg_strings;

    for (int i = 0; i < arguments.size(); ++i) {
      const zetasql::FunctionArgumentType& arg = arguments[i];
      std::string arg_string =
          absl::StrCat(arg.argument_name(), " ",
                       arg.type()->TypeName(zetasql::PRODUCT_EXTERNAL));
      if (arg.HasDefault()) {
        absl::StrAppend(&arg_string, " DEFAULT ",
                        arg.GetDefault().value().GetSQLLiteral(
                            zetasql::PRODUCT_EXTERNAL));
      }

      arg_strings.push_back(arg_string);
    }

    absl::StrAppend(&udf_string, "(", absl::StrJoin(arg_strings, ", "), ")");
    absl::StrAppend(
        &udf_string, " RETURNS ",
        signature->result_type().type()->TypeName(zetasql::PRODUCT_EXTERNAL));
  }

  if (udf->language() == Udf::Language::REMOTE) {
    absl::StrAppend(&udf_string, " LANGUAGE REMOTE");
  }
  if (udf->is_remote()) {
    absl::StrAppend(&udf_string, " REMOTE");
  }

  if (udf->security() == Udf::SqlSecurity::INVOKER) {
    absl::StrAppend(&udf_string, " SQL SECURITY INVOKER");
  }

  if (!udf->body().empty()) {
    absl::StrAppend(&udf_string, " AS (", udf->body(), ")");
    return udf_string;
  }

  std::string options = "";
  if (udf->endpoint().has_value()) {
    absl::StrAppend(&options, "endpoint='", *udf->endpoint(), "'");
  }
  if (udf->max_batching_rows().has_value()) {
    if (!options.empty()) {
      absl::StrAppend(&options, ", ");
    }
    absl::StrAppend(&options, "max_batching_rows=", *udf->max_batching_rows());
  }

  if (!options.empty()) {
    absl::StrAppend(&udf_string, " OPTIONS (", options, ")");
  }

  return udf_string;
}

std::string PrintNamedSchema(const NamedSchema* named_schema) {
  return absl::Substitute("CREATE SCHEMA $0", PrintName(named_schema->Name()));
}

std::string PrintProtoBundle(std::shared_ptr<const ProtoBundle> proto_bundle) {
  std::string proto_bundle_statement = "CREATE PROTO BUNDLE (\n";
  for (const auto& type : proto_bundle->types()) {
    absl::StrAppend(&proto_bundle_statement, "  ", type, ",\n");
  }
  absl::StrAppend(&proto_bundle_statement, ")");
  return proto_bundle_statement;
}

std::string PrintLocalityGroup(const LocalityGroup* locality_group) {
  std::string locality_group_statement = absl::Substitute(
      "CREATE LOCALITY GROUP $0", PrintName(locality_group->Name()));

  if (!locality_group->options().empty()) {
    absl::StrAppend(&locality_group_statement, " ", "OPTIONS ( ",
                    PrintLocalityGroupOptions(locality_group->options()), " )");
  }
  return locality_group_statement;
}

std::string PrintLocalityGroupOptions(
    ::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
  return absl::StrJoin(
      options, ", ", [](std::string* out, const ddl::SetOption& option) {
        if (option.option_name() ==
            ddl::kInternalLocalityGroupStorageOptionName) {
          absl::StrAppend(out, ddl::kLocalityGroupStorageOptionName, " = ");
          absl::StrAppend(out,
                          zetasql::ToSingleQuotedStringLiteral(
                              option.bool_value()
                                  ? ddl::kLocalityGroupStorageOptionSSDVal
                                  : ddl::kLocalityGroupStorageOptionHDDVal));
        } else if (option.option_name() ==
                   ddl::kInternalLocalityGroupSpillTimeSpanOptionName) {
          for (const auto& timeSpan : option.string_list_value()) {
            absl::string_view raw_time_span = timeSpan;
            if (absl::ConsumePrefix(&raw_time_span, "disk:")) {
              absl::StrAppend(out, ddl::kLocalityGroupSpillTimeSpanOptionName,
                              " = ");
              absl::StrAppend(
                  out, zetasql::ToSingleQuotedStringLiteral(raw_time_span));
            }
          }
        }
      });
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

  // Print database options.
  const DatabaseOptions* options = schema->options();
  if (options != nullptr) {
    for (const auto& option : options->options()) {
      statements.push_back(absl::Substitute(
          "ALTER DATABASE $0 SET OPTIONS ($1 = '$2')", options->Name(),
          option.option_name(), option.string_value()));
    }
  }

  // Print proto bundle.
  std::shared_ptr<const ProtoBundle> proto_bundle = schema->proto_bundle();
  if (proto_bundle != nullptr && !proto_bundle->types().empty()) {
    statements.push_back(PrintProtoBundle(proto_bundle));
  }

  // Print schema nodes while ensuring that dependencies are printed first.
  absl::flat_hash_set<const SchemaNode*> visited;
  for (const NamedSchema* named_schema : schema->named_schemas()) {
    TopologicalOrderSchemaNodes(named_schema, &visited, &statements);
  }
  for (const Placement* placement : schema->placements()) {
    TopologicalOrderSchemaNodes(placement, &visited, &statements);
  }
  for (const Sequence* sequence : schema->user_visible_sequences()) {
    TopologicalOrderSchemaNodes(sequence, &visited, &statements);
  }
  for (const Table* table : schema->tables()) {
    TopologicalOrderSchemaNodes(table, &visited, &statements);
  }
  for (const ChangeStream* change_stream : schema->change_streams()) {
    TopologicalOrderSchemaNodes(change_stream, &visited, &statements);
  }
  for (const Model* model : schema->models()) {
    TopologicalOrderSchemaNodes(model, &visited, &statements);
  }
  for (const View* view : schema->views()) {
    TopologicalOrderSchemaNodes(view, &visited, &statements);
  }
  for (const Udf* udf : schema->udfs()) {
    TopologicalOrderSchemaNodes(udf, &visited, &statements);
  }
  for (const PropertyGraph* property_graph : schema->property_graphs()) {
    TopologicalOrderSchemaNodes(property_graph, &visited, &statements);
  }

  for (auto locality_group : schema->locality_groups()) {
    // The default locality group will be added to the schema
    // only when it has been altered by the user.
    if (!IsSystemLocalityGroup(locality_group->Name())) {
      statements.push_back(PrintLocalityGroup(locality_group));
    }
  }

  return statements;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
