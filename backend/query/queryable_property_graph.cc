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

#include "backend/query/queryable_property_graph.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/analyzer_options.h"
#include "backend/schema/catalog/property_graph.h"
#include "common/constants.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Helper function to match up the column names in a GraphElementTable to the
// columns in the underlying Table.
absl::Status MatchGraphTableColumnHelper(
    const zetasql::Table* source_table,
    absl::Span<const std::string> column_names,
    std::vector<int>& underlying_column_indices) {
  ZETASQL_RET_CHECK_NE(source_table, nullptr);
  underlying_column_indices.reserve(column_names.size());
  for (absl::string_view col_name : column_names) {
    const int num_data_source_cols = source_table->NumColumns();
    for (int i = 0; i < num_data_source_cols; ++i) {
      const zetasql::Column* source_col = source_table->GetColumn(i);
      if (absl::EqualsIgnoreCase(source_col->Name(), col_name)) {
        underlying_column_indices.push_back(i);
      }
    }
  }
  ZETASQL_RET_CHECK(underlying_column_indices.size() == column_names.size())
      << "Column mismatch between GraphElementTable and its underlying Table. "
      << ". Underlying table: " << source_table->Name();
  return absl::OkStatus();
}

QueryableGraphPropertyDeclaration::QueryableGraphPropertyDeclaration(
    const QueryablePropertyGraph* property_graph, zetasql::Catalog* catalog,
    zetasql::TypeFactory* type_factory,
    const google::spanner::emulator::backend::PropertyGraph::
        PropertyDeclaration* wrapped_property_declaration)
    : property_graph_(property_graph),
      wrapped_property_declaration_(wrapped_property_declaration) {
  const zetasql::Type* type;
  zetasql::AnalyzerOptions analyzer_options =
      MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone);
  absl::Status analyze_status =
      zetasql::AnalyzeType(wrapped_property_declaration->type,
                             analyzer_options, catalog, type_factory, &type);
  if (!analyze_status.ok()) {
    ABSL_LOG(FATAL) << "Failed to analyze property type: "
               << wrapped_property_declaration->type;
  }
  type_ = type;
}

std::string QueryableGraphPropertyDeclaration::FullName() const {
  return absl::StrCat(property_graph_->FullName(), ".", Name());
}

QueryableGraphPropertyDefinition::QueryableGraphPropertyDefinition(
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
    const zetasql::Table* data_source_table,
    const zetasql::GraphPropertyDeclaration* property_declaration,
    const google::spanner::emulator::backend::PropertyGraph::GraphElementTable::
        PropertyDefinition* wrapped_property_definition)
    : property_declaration_(property_declaration),
      wrapped_property_definition_(wrapped_property_definition) {
  zetasql::AnalyzerOptions analyzer_options =
      MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone);
  // Setup a callback for ZetaSQL's resolver to be able to map the property
  // definition expression back to existing columns in the catalog.
  zetasql::AnalyzerOptions local_options = analyzer_options;
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;
  local_options.SetLookupCatalogColumnCallback(
      [data_source_table](const std::string& column_name)
          -> absl::StatusOr<const zetasql::Column*> {
        const zetasql::Column* column =
            data_source_table->FindColumnByName(column_name);
        if (column == nullptr) {
          return absl::NotFoundError(
              absl::StrCat("Cannot find column named ", column_name));
        }
        return column;
      });

  // b/307318516: As we re-analyze each property definition expression below,
  // its parse locations are unlikely to be accurate. This could cause issues
  // while accessing these properties (for example, in the literal_remover).
  // Parse locations are also primarily used for providing better error
  // messages, hence we turn off parse location recording.
  local_options.set_parse_location_record_type(
      zetasql::ParseLocationRecordType::PARSE_LOCATION_RECORD_NONE);
  // Analyze the expression and store the resulting ResolvedExpr in the
  // catalog.
  absl::Status analyze_status = zetasql::AnalyzeExpressionForAssignmentToType(
      wrapped_property_definition->value_expression_string, local_options,
      catalog, type_factory, property_declaration->Type(), &analyzer_output_);
  if (!analyze_status.ok()) {
    ABSL_LOG(FATAL) << "Failed to analyze property definition expression: "
               << wrapped_property_definition->value_expression_string;
  }
}

std::string QueryableGraphElementLabel::FullName() const {
  return absl::StrCat(property_graph_->FullName(), ".", Name());
}

absl::Status QueryableGraphElementLabel::GetPropertyDeclarations(
    absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>& output)
    const {
  for (const auto& property_name : label_->property_names) {
    const zetasql::GraphPropertyDeclaration* property_declaration = nullptr;
    ZETASQL_RETURN_IF_ERROR(property_graph_->FindPropertyDeclarationByName(
        property_name, property_declaration));
    ABSL_CHECK_NE(property_declaration, nullptr)
        << "Property declaration not found: " << property_name;
    output.insert(property_declaration);
  }
  return absl::OkStatus();
}

QueryableGraphElementTableInternal::QueryableGraphElementTableInternal(
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
    const QueryablePropertyGraph* property_graph,
    const google::spanner::emulator::backend::PropertyGraph::GraphElementTable*
        wrapped_element_table)
    : property_graph_(property_graph),
      wrapped_element_table_(wrapped_element_table) {
  for (const auto& label_name : wrapped_element_table_->label_names()) {
    const zetasql::GraphElementLabel* label = nullptr;
    absl::Status find_status =
        property_graph_->FindLabelByName(label_name, label);
    if (!find_status.ok()) {
      ABSL_LOG(FATAL) << "Element Label not found in property graph: " << label_name;
    }
    label_ptrs_[label_name] = label;
  }

  const zetasql::Table* data_source_table;
  std::string data_source_table_name = wrapped_element_table_->name();
  absl::Status find_status =
      catalog->FindTable({data_source_table_name}, &data_source_table);
  if (!find_status.ok()) {
    ABSL_LOG(FATAL) << "Data source table not found in catalog: "
               << data_source_table_name;
  }
  data_source_table_ = data_source_table;

  // Match up the key columns to find the position of the referenced column
  // in the underlying source table.
  absl::Status match_status = MatchGraphTableColumnHelper(
      data_source_table_,
      absl::MakeSpan(wrapped_element_table_->key_clause_columns()),
      ordinal_key_columns_idxs_);
  if (!match_status.ok()) {
    ABSL_LOG(FATAL) << "Failed to match graph table columns: " << match_status;
  }

  for (const auto& property_definition :
       wrapped_element_table_->property_definitions()) {
    const zetasql::GraphPropertyDeclaration* property_declaration = nullptr;
    absl::Status find_status = property_graph_->FindPropertyDeclarationByName(
        property_definition.name, property_declaration);
    if (find_status.ok()) {
      property_definitions_[property_definition.name] =
          std::make_unique<QueryableGraphPropertyDefinition>(
              catalog, type_factory, data_source_table, property_declaration,
              &property_definition);
    }
  }
}

const zetasql::Table* QueryableGraphElementTableInternal::GetTable() const {
  return data_source_table_;
}

absl::Span<const std::string>
QueryableGraphElementTableInternal::PropertyGraphNamePath() const {
  return property_graph_->NamePath();
}

std::string QueryableGraphElementTableInternal::FullName() const {
  return absl::StrCat(property_graph_->FullName(), ".", Name());
}

absl::Status QueryableGraphElementTableInternal::FindPropertyDefinitionByName(
    absl::string_view property_name,
    const zetasql::GraphPropertyDefinition*& property_definition) const {
  auto it = property_definitions_.find(std::string(property_name));
  if (it == property_definitions_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Property definition not found: ", property_name));
  }
  property_definition = it->second.get();
  return absl::OkStatus();
}

absl::Status QueryableGraphElementTableInternal::GetPropertyDefinitions(
    absl::flat_hash_set<const zetasql::GraphPropertyDefinition*>& output)
    const {
  for (const auto& [_, property_definition] : property_definitions_) {
    output.insert(property_definition.get());
  }
  return absl::OkStatus();
}

absl::Status QueryableGraphElementTableInternal::FindLabelByName(
    absl::string_view name, const zetasql::GraphElementLabel*& label) const {
  auto it = label_ptrs_.find(std::string(name));
  if (it == label_ptrs_.end()) {
    return absl::NotFoundError(absl::StrCat("Label not found: ", name));
  }
  label = it->second;
  return absl::OkStatus();
}

absl::Status QueryableGraphElementTableInternal::GetLabels(
    absl::flat_hash_set<const zetasql::GraphElementLabel*>& output) const {
  for (const auto& [_, label] : label_ptrs_) {
    output.insert(label);
  }
  return absl::OkStatus();
}

QueryableGraphNodeTable::QueryableGraphNodeTable(
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
    const QueryablePropertyGraph* property_graph,
    const google::spanner::emulator::backend::PropertyGraph::GraphElementTable*
        node_table)
    : element_table_internals_(
          std::make_unique<QueryableGraphElementTableInternal>(
              catalog, type_factory, property_graph, node_table)) {}

QueryableGraphNodeTableReference::QueryableGraphNodeTableReference(
    zetasql::Catalog* catalog, const QueryablePropertyGraph* property_graph,
    const google::spanner::emulator::backend::PropertyGraph::GraphElementTable::
        GraphNodeReference* wrapped_node_reference,
    const google::spanner::emulator::backend::PropertyGraph::GraphElementTable*
        wrapped_edge_table)
    : property_graph_(property_graph),
      wrapped_node_reference_(wrapped_node_reference) {
  const zetasql::Table* referenced_node_table;
  absl::Status find_status = catalog->FindTable(
      {wrapped_node_reference_->node_table_name}, &referenced_node_table);
  if (!find_status.ok()) {
    ABSL_LOG(FATAL) << "Data source table not found in catalog: "
               << wrapped_node_reference_->node_table_name;
  }

  absl::Status match_status = MatchGraphTableColumnHelper(
      referenced_node_table,
      absl::MakeSpan(wrapped_node_reference_->node_table_column_names),
      node_table_column_idxs_);
  if (!match_status.ok()) {
    ABSL_LOG(FATAL) << "Failed to match graph table columns: " << match_status;
  }

  const zetasql::Table* referencing_edge_table;
  find_status =
      catalog->FindTable({wrapped_edge_table->name()}, &referencing_edge_table);
  if (!find_status.ok()) {
    ABSL_LOG(FATAL) << "Data source table not found in catalog: "
               << wrapped_edge_table->name();
  }

  match_status = MatchGraphTableColumnHelper(
      referencing_edge_table,
      absl::MakeSpan(wrapped_node_reference_->edge_table_column_names),
      edge_table_column_idxs_);
  if (!match_status.ok()) {
    ABSL_LOG(FATAL) << "Failed to match graph table columns: " << match_status;
  }
}

const zetasql::GraphNodeTable*
QueryableGraphNodeTableReference::GetReferencedNodeTable() const {
  const zetasql::GraphElementTable* element_table = nullptr;
  absl::Status status = property_graph_->FindElementTableByName(
      wrapped_node_reference_->node_table_name, element_table);
  // DDL Validation ensures that the referenced node table exists.
  ABSL_CHECK_OK(status);
  return element_table->AsNodeTable();
}

QueryableGraphEdgeTable::QueryableGraphEdgeTable(
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
    const QueryablePropertyGraph* property_graph,
    const google::spanner::emulator::backend::PropertyGraph::GraphElementTable*
        edge_table)
    : element_table_internals_(
          std::make_unique<QueryableGraphElementTableInternal>(
              catalog, type_factory, property_graph, edge_table)),
      source_node_table_reference_(
          std::make_unique<QueryableGraphNodeTableReference>(
              catalog, property_graph, &edge_table->source_node_reference(),
              edge_table)),
      target_node_table_reference_(
          std::make_unique<QueryableGraphNodeTableReference>(
              catalog, property_graph, &edge_table->target_node_reference(),
              edge_table)) {}

}  // namespace backend

}  // namespace emulator
}  // namespace spanner
}  // namespace google
