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

#include "zetasql/public/catalog.h"

#include <cassert>
#include <memory>
#include <string>

#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/function.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/queryable_named_schema.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/schema.h"
#include "common/constants.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using postgres_translator::spangres::datatypes::GetPgJsonbType;
using postgres_translator::spangres::datatypes::GetPgNumericType;
using postgres_translator::spangres::datatypes::GetPgOidType;
using ::testing::Contains;
using ::testing::Property;
using ::zetasql_base::testing::StatusIs;

// An integration test that uses Catalog to call zetasql::AnalyzeStatement.
class AnalyzeStatementTest : public testing::Test {
 protected:
  absl::Status AnalyzeStatement(
      absl::string_view sql,
      DatabaseDialect dialect = DatabaseDialect::GOOGLE_STANDARD_SQL) {
    if (dialect == DatabaseDialect::POSTGRESQL) {
      // Initialize the Spangres system catalog for pg_catalog.
      postgres_translator::spangres::test::GetSpangresTestSystemCatalog(
          MakeGoogleSqlLanguageOptions());
    }
    zetasql::TypeFactory type_factory{};
    std::unique_ptr<const Schema> schema =
        test::CreateSchemaWithOneTable(&type_factory, dialect);
    FunctionCatalog function_catalog{
        &type_factory,
        /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
        /*latest_schema=*/schema.get()};
    function_catalog.SetLatestSchema(schema.get());
    auto analyzer_options = MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone);
    Catalog catalog{schema.get(), &function_catalog, &type_factory,
                    analyzer_options};
    std::unique_ptr<const zetasql::AnalyzerOutput> output{};
    return zetasql::AnalyzeStatement(sql, analyzer_options, &catalog,
                                       &type_factory, &output);
  }
};

TEST_F(AnalyzeStatementTest, SelectOneFromExistingTableReturnsOk) {
  ZETASQL_EXPECT_OK(AnalyzeStatement("SELECT 1 FROM test_table"));
}

TEST_F(AnalyzeStatementTest, SelectOneFromNonexistentTableReturnsError) {
  EXPECT_THAT(AnalyzeStatement("SELECT 1 FROM prod_table"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(AnalyzeStatementTest, SelectExistingColumnReturnsOk) {
  ZETASQL_EXPECT_OK(AnalyzeStatement("SELECT int64_col FROM test_table"));
}

TEST_F(AnalyzeStatementTest, SelectNonexistentColumnReturnsError) {
  EXPECT_THAT(AnalyzeStatement("SELECT json_col FROM test_table"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(AnalyzeStatementTest, SelectNestedCatalogNetFunctions) {
  ZETASQL_EXPECT_OK(
      AnalyzeStatement("SELECT "
                       "NET.IPV4_TO_INT64(b\"\\x00\\x00\\x00\\x00\")"));
}

TEST_F(AnalyzeStatementTest, SelectNestedCatalogPGFunctions) {
  ZETASQL_EXPECT_OK(
      AnalyzeStatement("SELECT pg.map_double_to_int(CAST(1.1 as float64)) "
                       "IN (pg.map_double_to_int(1.1), "
                       "pg.map_double_to_int(2.0)) as col"));
}

TEST_F(AnalyzeStatementTest, SelectIdentityColumnFieldsInformationSchema) {
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "SELECT is_identity, identity_generation, identity_kind, "
      "identity_start_with_counter, identity_skip_range_min, "
      "identity_skip_range_max FROM information_schema.columns"));
}

TEST_F(AnalyzeStatementTest, SelectIdentityColumnFieldsPGInformationSchema) {
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "SELECT is_identity, identity_generation, identity_kind, "
      "identity_start_with_counter, identity_skip_range_min, "
      "identity_skip_range_max FROM pg_information_schema.columns"));
}

TEST_F(AnalyzeStatementTest, SelectFromPGInformationSchema) {
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "SELECT column_name FROM pg_information_schema.columns"));
}

TEST_F(AnalyzeStatementTest, SelectFromPGCatalog_ZetaSQL) {
  // pg_catalog is not available in ZetaSQL.
  EXPECT_THAT(AnalyzeStatement("SELECT tablename FROM pg_catalog.pg_tables"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(AnalyzeStatementTest, SelectFromPGCatalog_PostgreSQL) {
  ZETASQL_EXPECT_OK(AnalyzeStatement("SELECT tablename FROM pg_catalog.pg_tables",
                             DatabaseDialect::POSTGRESQL));
}

class CatalogTest : public testing::Test {
 public:
  CatalogTest() : type_factory_() {}

  void SetUp() override {
    schema_ = test::CreateSchemaWithOneTable(&type_factory_);
    function_catalog_ = std::make_unique<FunctionCatalog>(
        &type_factory_,
        /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
        /*latest_schema=*/schema_.get());
    catalog_ = std::make_unique<Catalog>(
        schema_.get(), function_catalog_.get(), &type_factory_,
        MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone));
  }

  void MakeCatalog(absl::Span<const std::string> statements,
                   database_api::DatabaseDialect dialect =
                       database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema_, test::CreateSchemaFromDDL(
                                      statements, &type_factory_, "", dialect));
    catalog_ = std::make_unique<Catalog>(
        schema_.get(), function_catalog_.get(), &type_factory_,
        MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone));
  }

  void MakeChangeStreamInternalQueryCatalog(
      absl::Span<const std::string> statements) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema_,
                         test::CreateSchemaFromDDL(statements, &type_factory_));
    catalog_ = std::make_unique<Catalog>(
        schema_.get(), function_catalog_.get(), &type_factory_,
        MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone));
    change_stream_internal_query_catalog_ = std::make_unique<Catalog>(
        schema_.get(), function_catalog_.get(), &type_factory_,
        MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone), /*reader=*/nullptr,
        /*query_evaluator=*/nullptr,
        /*change_stream_internal_lookup=*/"test_stream");
  }

 protected:
  zetasql::EnumerableCatalog& catalog() { return *catalog_; }
  zetasql::EnumerableCatalog& change_stream_internal_query_catalog() {
    return *change_stream_internal_query_catalog_;
  }

 private:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<test::ScopedEmulatorFeatureFlagsSetter> flag_setter_;
  std::unique_ptr<const Schema> schema_ = nullptr;
  std::unique_ptr<FunctionCatalog> function_catalog_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<Catalog> change_stream_internal_query_catalog_;
};

TEST_F(CatalogTest, FullNameNameIsAlwaysEmpty) {
  EXPECT_EQ(catalog().FullName(), "");
}

TEST_F(CatalogTest, FindTableTableIsFound) {
  const zetasql::Table* table;
  ZETASQL_EXPECT_OK(catalog().FindTable({"test_table"}, &table, {}));
  EXPECT_EQ(table->FullName(), "test_table");
}

TEST_F(CatalogTest, FindTableTableIsNotFound) {
  const zetasql::Table* table;
  EXPECT_THAT(catalog().FindTable({"BAR"}, &table, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(table, nullptr);
}

TEST_F(CatalogTest, FindFunctionFindsCountFunction) {
  const zetasql::Function* function;
  ZETASQL_ASSERT_OK(catalog().FindFunction({"COUNT"}, &function, {}));
  EXPECT_EQ(function->Name(), "count");
}

TEST_F(CatalogTest, FindFunctionFindsSoundexFunction) {
  const zetasql::Function* function;
  ZETASQL_ASSERT_OK(catalog().FindFunction({"SOUNDEX"}, &function, {}));
  EXPECT_EQ(function->Name(), "soundex");
}

TEST_F(CatalogTest, FunctionOptionsAreUpdatedForPGFunctions) {
  const zetasql::Function* function;

  ZETASQL_ASSERT_OK(catalog().FindFunction({"pg.least"}, &function, {}));
  EXPECT_NE(function->GetFunctionEvaluatorFactory(), nullptr);
  EXPECT_EQ(function->GetAggregateFunctionEvaluatorFactory(), nullptr);

  ZETASQL_ASSERT_OK(catalog().FindFunction({"pg.min"}, &function, {}));
  EXPECT_EQ(function->GetFunctionEvaluatorFactory(), nullptr);
  EXPECT_NE(function->GetAggregateFunctionEvaluatorFactory(), nullptr);
}

TEST_F(CatalogTest,
       FindFunctionDoesNotFindNonSoundexAdditionalStringFunctions) {
  const zetasql::Function* function;
  EXPECT_THAT(catalog().FindFunction({"INSTR"}, &function, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_THAT(catalog().FindFunction({"TRANSLATE"}, &function, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_THAT(catalog().FindFunction({"INITCAP"}, &function, {}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(CatalogTest, GetTablesGetsTheOnlyTable) {
  using zetasql::Table;
  absl::flat_hash_set<const Table*> output;
  ZETASQL_EXPECT_OK(catalog().GetTables(&output));
  EXPECT_EQ(output.size(), 1);
  EXPECT_THAT(output, Contains(Property(&Table::Name, "test_table")));
}

TEST_F(CatalogTest, FindViewTable) {
  test::ScopedEmulatorFeatureFlagsSetter flag_setter({
      .enable_views = true,
  });
  MakeCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE VIEW test_view SQL SECURITY INVOKER AS
        SELECT t.string_col AS view_col FROM test_table t
      )",
  });

  const zetasql::Table* view;
  ZETASQL_EXPECT_OK(catalog().FindTable({"test_view"}, &view, {}));
  EXPECT_EQ(view->FullName(), "test_view");

  EXPECT_EQ(view->NumColumns(), 1);
  auto view_col = view->FindColumnByName("view_col");
  EXPECT_NE(view_col, nullptr);
  EXPECT_EQ(view_col->FullName(), "test_view.view_col");
  EXPECT_EQ(view_col->IsWritableColumn(), false);
  EXPECT_EQ(view_col->IsPseudoColumn(), false);
  EXPECT_EQ(view_col->GetType(), zetasql::types::StringType());
  EXPECT_EQ(view->FindColumnByName("view_col"), view_col);
}

TEST_F(CatalogTest, FindBuiltInTableValuedFunction) {
  const zetasql::TableValuedFunction* tvf;
  ZETASQL_EXPECT_OK(catalog().FindTableValuedFunction({"ML.PREDICT"}, &tvf, {}));
  EXPECT_EQ(tvf->FullName(), "ML.PREDICT");
  ZETASQL_EXPECT_OK(catalog().FindTableValuedFunction({"SAFE.ML.PREDICT"}, &tvf, {}));
  EXPECT_EQ(tvf->FullName(), "SAFE.ML.PREDICT");
  ZETASQL_EXPECT_OK(
      catalog().FindTableValuedFunction({"pg.jsonb_array_elements"}, &tvf, {}));
  EXPECT_EQ(tvf->FullName(), "pg.jsonb_array_elements");
}

TEST_F(CatalogTest, FindTableValuedFunction) {
  MakeCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE CHANGE STREAM test_stream FOR test_table
      )",
  });

  const zetasql::TableValuedFunction* tvf;
  ZETASQL_EXPECT_OK(catalog().FindTableValuedFunction({"READ_test_stream"}, &tvf, {}));
  EXPECT_EQ(tvf->FullName(), "READ_test_stream");
}

TEST_F(CatalogTest, FindTableValuedFunctionIsNotFound) {
  MakeCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE CHANGE STREAM test_stream FOR test_table
      )",
  });

  const zetasql::TableValuedFunction* tvf;
  EXPECT_THAT(catalog().FindTableValuedFunction({"BAR_tvf"}, &tvf, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(tvf, nullptr);
}

TEST_F(CatalogTest, FindChangeStreamInternalPartitionTable) {
  MakeChangeStreamInternalQueryCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE CHANGE STREAM test_stream FOR test_table
      )",
  });

  const zetasql::Table* partition_table;
  ZETASQL_EXPECT_OK(change_stream_internal_query_catalog().FindTable(
      {"_change_stream_partition_test_stream"}, &partition_table, {}));
  EXPECT_EQ(partition_table->FullName(),
            "_change_stream_partition_test_stream");
}

TEST_F(CatalogTest, FindChangeStreamInternalDataTable) {
  MakeChangeStreamInternalQueryCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE CHANGE STREAM test_stream FOR test_table
      )",
  });

  const zetasql::Table* data_table;
  ZETASQL_EXPECT_OK(change_stream_internal_query_catalog().FindTable(
      {"_change_stream_data_test_stream"}, &data_table, {}));
  EXPECT_EQ(data_table->FullName(), "_change_stream_data_test_stream");
}

TEST_F(CatalogTest,
       FindChangeStreamInternalPartitionTableNotValidFromExternalUser) {
  MakeCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE CHANGE STREAM test_stream FOR test_table
      )",
  });

  const zetasql::Table* partition_table;
  EXPECT_THAT(catalog().FindTable({"_change_stream_partition_test_stream"},
                                  &partition_table, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(partition_table, nullptr);
}

TEST_F(CatalogTest, FindChangeStreamInternalDataTableNotValidFromExternalUser) {
  MakeCatalog({
      R"(
        CREATE TABLE test_table (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(
        CREATE CHANGE STREAM test_stream FOR test_table
      )",
  });

  const zetasql::Table* data_table;
  EXPECT_THAT(
      catalog().FindTable({"_change_stream_dat_test_stream"}, &data_table, {}),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(data_table, nullptr);
}

TEST_F(CatalogTest, FindSequence) {
  test::ScopedEmulatorFeatureFlagsSetter flag_setter({
      .enable_bit_reversed_positive_sequences = true,
  });
  MakeCatalog({
      R"(
        CREATE SEQUENCE myseq OPTIONS (
          sequence_kind = "bit_reversed_positive",
          start_with_counter = 500
        )
      )",
      R"(
        CREATE SEQUENCE myseq2 OPTIONS (
          sequence_kind = "bit_reversed_positive",
          skip_range_min = 1,
          skip_range_max = 1000
        )
      )",
      R"(
        CREATE TABLE test_table (
          int64_col INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
  });

  const zetasql::Sequence* myseq;
  ZETASQL_EXPECT_OK(catalog().FindSequence({"myseq"}, &myseq, {}));
  EXPECT_NE(myseq, nullptr);
  EXPECT_EQ(myseq->Name(), "myseq");
  const zetasql::Sequence* myseq2;
  ZETASQL_EXPECT_OK(catalog().FindSequence({"myseq2"}, &myseq2, {}));
  EXPECT_NE(myseq2, nullptr);
  EXPECT_EQ(myseq2->Name(), "myseq2");
  const zetasql::Sequence* myseq3;
  EXPECT_THAT(catalog().FindSequence({"myseq3"}, &myseq3, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(myseq3, nullptr);
}

TEST_F(CatalogTest, FindPropertyGraph) {
  MakeCatalog({
      R"(
        CREATE TABLE node_test(
          Id INT64 NOT NULL,
        ) PRIMARY KEY(Id)
      )",
      R"(
        CREATE TABLE edge_test(
          FromId INT64 NOT NULL,
          ToId INT64 NOT NULL,
        ) PRIMARY KEY(FromId, ToId)
      )",
      R"(
        CREATE PROPERTY GRAPH test_graph
            NODE TABLES(
              node_test KEY(Id)
                LABEL NodeTest PROPERTIES(Id)
            )
            EDGE TABLES(
              edge_test
                KEY(FromId, ToId)
                SOURCE KEY(FromId) REFERENCES node_test(Id)
                DESTINATION KEY(ToId) REFERENCES node_test(Id)
                DEFAULT LABEL PROPERTIES ALL COLUMNS EXCEPT (ToId)
                LABEL EdgeNoPropLabel NO PROPERTIES
            )
      )",
  });
  // Test property graph.
  const zetasql::PropertyGraph* graph;
  ZETASQL_EXPECT_OK(catalog().FindPropertyGraph({"test_graph"}, graph, {}));
  EXPECT_EQ(graph->FullName(), "test_graph");

  absl::flat_hash_set<const zetasql::GraphElementLabel*> all_graph_labels;
  ZETASQL_EXPECT_OK(graph->GetLabels(all_graph_labels));
  EXPECT_EQ(all_graph_labels.size(), 3);

  // Test labels from graph.
  {
    const zetasql::GraphElementLabel* graph_label;
    ZETASQL_EXPECT_OK(graph->FindLabelByName("NodeTest", graph_label));
    EXPECT_NE(graph_label, nullptr);
    EXPECT_EQ(graph_label->FullName(), "test_graph.NodeTest");

    absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>
        property_declarations_from_label;
    ZETASQL_EXPECT_OK(
        graph_label->GetPropertyDeclarations(property_declarations_from_label));
    EXPECT_EQ(property_declarations_from_label.size(), 1);
    const zetasql::GraphPropertyDeclaration* decl_from_label =
        *property_declarations_from_label.begin();
    EXPECT_EQ(decl_from_label->FullName(), "test_graph.Id");
  }
  {
    const zetasql::GraphElementLabel* graph_label;
    ZETASQL_EXPECT_OK(graph->FindLabelByName("EdgeNoPropLabel", graph_label));
    EXPECT_NE(graph_label, nullptr);
    EXPECT_EQ(graph_label->FullName(), "test_graph.EdgeNoPropLabel");

    // Ensure there are NO PROPERTIES
    absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>
        property_declarations_from_label;
    ZETASQL_EXPECT_OK(
        graph_label->GetPropertyDeclarations(property_declarations_from_label));
    EXPECT_EQ(property_declarations_from_label.size(), 0);
  }
  {
    const zetasql::GraphElementLabel* graph_label;
    ZETASQL_EXPECT_OK(graph->FindLabelByName("edge_test", graph_label));
    EXPECT_NE(graph_label, nullptr);
    EXPECT_EQ(graph_label->FullName(), "test_graph.edge_test");

    // Ensure all PROPERTIES are linked to this label EXCEPT (ToId)
    absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>
        property_declarations_from_label;
    ZETASQL_EXPECT_OK(
        graph_label->GetPropertyDeclarations(property_declarations_from_label));
    EXPECT_EQ(property_declarations_from_label.size(), 1);
    const zetasql::GraphPropertyDeclaration* decl_from_label =
        *property_declarations_from_label.begin();
    EXPECT_EQ(decl_from_label->FullName(), "test_graph.FromId");
  }

  // Test property declarations from graph.
  const zetasql::GraphPropertyDeclaration* property_declaration;
  ZETASQL_EXPECT_OK(graph->FindPropertyDeclarationByName("Id", property_declaration));
  EXPECT_NE(property_declaration, nullptr);
  EXPECT_EQ(property_declaration->FullName(), "test_graph.Id");
  EXPECT_EQ(property_declaration->Type(), zetasql::types::Int64Type());

  // Test node tables from graph.
  absl::flat_hash_set<const zetasql::GraphNodeTable*> node_tables;
  ZETASQL_EXPECT_OK(graph->GetNodeTables(node_tables));
  EXPECT_EQ(node_tables.size(), 1);
  const zetasql::GraphNodeTable* node_table = *node_tables.begin();
  EXPECT_EQ(node_table->FullName(), "test_graph.node_test");
  EXPECT_EQ(node_table->Name(), "node_test");
  EXPECT_EQ(node_table->kind(), zetasql::GraphNodeTable::Kind::kNode);
  EXPECT_EQ(absl::StrJoin(node_table->PropertyGraphNamePath(), "."),
            "test_graph");
  EXPECT_NE(node_table->GetTable(), nullptr);
  EXPECT_EQ(node_table->GetKeyColumns().size(), 1);
  absl::flat_hash_set<const zetasql::GraphPropertyDefinition*> node_prop_defs;
  ZETASQL_EXPECT_OK(node_table->GetPropertyDefinitions(node_prop_defs));
  EXPECT_EQ(node_prop_defs.size(), 1);
  absl::flat_hash_set<const zetasql::GraphElementLabel*> node_labels;
  ZETASQL_EXPECT_OK(node_table->GetLabels(node_labels));
  EXPECT_EQ(node_labels.size(), 1);

  const zetasql::GraphElementLabel* node_label;
  ZETASQL_EXPECT_OK(node_table->FindLabelByName("NodeTest", node_label));
  EXPECT_NE(node_label, nullptr);

  // Test property declarations from node table's label.
  absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>
      property_declarations_from_node_label;
  ZETASQL_EXPECT_OK(node_label->GetPropertyDeclarations(
      property_declarations_from_node_label));
  EXPECT_EQ(property_declarations_from_node_label.size(), 1);
  const zetasql::GraphPropertyDeclaration* decl_from_node_label =
      *property_declarations_from_node_label.begin();
  EXPECT_EQ(decl_from_node_label->FullName(), "test_graph.Id");

  // Test property definitions from node table.
  const zetasql::GraphPropertyDefinition* property_definition;
  ZETASQL_EXPECT_OK(
      node_table->FindPropertyDefinitionByName("Id", property_definition));
  EXPECT_NE(property_definition, nullptr);
  EXPECT_EQ(property_definition->expression_sql(), "Id");
  EXPECT_NE(property_definition->GetValueExpression().value(), nullptr);

  // Test edge tables from graph.
  absl::flat_hash_set<const zetasql::GraphEdgeTable*> edge_tables;
  ZETASQL_EXPECT_OK(graph->GetEdgeTables(edge_tables));
  EXPECT_EQ(edge_tables.size(), 1);
  const zetasql::GraphEdgeTable* edge_table = *edge_tables.begin();
  EXPECT_EQ(edge_table->FullName(), "test_graph.edge_test");
  EXPECT_EQ(edge_table->Name(), "edge_test");
  EXPECT_EQ(edge_table->kind(), zetasql::GraphEdgeTable::Kind::kEdge);

  // Test source and destination node table references.
  const zetasql::GraphNodeTableReference* source_node_table_reference =
      edge_table->GetSourceNodeTable();
  EXPECT_NE(source_node_table_reference, nullptr);
  EXPECT_NE(source_node_table_reference->GetReferencedNodeTable(), nullptr);
  EXPECT_EQ(source_node_table_reference->GetReferencedNodeTable()->FullName(),
            "test_graph.node_test");
  EXPECT_GE(source_node_table_reference->GetEdgeTableColumns().size(), 1);
  EXPECT_GE(source_node_table_reference->GetNodeTableColumns().size(), 1);

  const zetasql::GraphNodeTableReference* dest_node_table_reference =
      edge_table->GetDestNodeTable();
  EXPECT_NE(dest_node_table_reference, nullptr);
  EXPECT_NE(dest_node_table_reference->GetReferencedNodeTable(), nullptr);
  EXPECT_EQ(dest_node_table_reference->GetReferencedNodeTable()->FullName(),
            "test_graph.node_test");
  EXPECT_GE(dest_node_table_reference->GetEdgeTableColumns().size(), 1);
  EXPECT_GE(dest_node_table_reference->GetNodeTableColumns().size(), 1);

  // Test each public API for the NOT FOUND cases
  const zetasql::GraphElementTable* not_found_graph_element_table;
  EXPECT_THAT(graph->FindElementTableByName("NONEXISTENT_ELEMENT",
                                            not_found_graph_element_table),
              StatusIs(absl::StatusCode::kNotFound));

  const zetasql::GraphElementLabel* not_found_graph_label;
  EXPECT_THAT(
      graph->FindLabelByName("NONEXISTENT_LABEL", not_found_graph_label),
      StatusIs(absl::StatusCode::kNotFound));

  const zetasql::GraphPropertyDeclaration* not_found_property_declaration;
  EXPECT_THAT(graph->FindPropertyDeclarationByName(
                  "NONEXISTENT_PROPERTY_DECL", not_found_property_declaration),
              StatusIs(absl::StatusCode::kNotFound));

  const zetasql::GraphPropertyDefinition* not_found_property_definition;
  EXPECT_THAT(node_table->FindPropertyDefinitionByName(
                  "NONEXISTENT_PROPERTY", not_found_property_definition),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(CatalogTest, FindPropertyGraphWithDynamicLabelAndProperties) {
  MakeCatalog({
      R"(
    CREATE TABLE GraphNode (
      id INT64 NOT NULL,
      label STRING(MAX) NOT NULL,
      properties JSON,
    ) PRIMARY KEY (id)
  )",
      R"(
    CREATE TABLE GraphEdge (
      id INT64 NOT NULL,
      dest_id INT64 NOT NULL,
      edge_id INT64 NOT NULL,
      label STRING(MAX) NOT NULL,
      properties JSON,
    ) PRIMARY KEY (id, dest_id, edge_id),
      INTERLEAVE IN PARENT GraphNode
  )",
      R"(
    CREATE OR REPLACE PROPERTY GRAPH aml
      NODE TABLES (
        GraphNode
          DYNAMIC LABEL (label)
          DYNAMIC PROPERTIES (properties)
      )
      EDGE TABLES (
        GraphEdge
          SOURCE KEY (id) REFERENCES GraphNode(id)
          DESTINATION KEY (dest_id) REFERENCES GraphNode(id)
          DYNAMIC LABEL (label)
          DYNAMIC PROPERTIES (properties)
      )
  )",
  });
  // Test property graph.
  const zetasql::PropertyGraph* graph;
  ZETASQL_EXPECT_OK(catalog().FindPropertyGraph({"aml"}, graph, {}));
  EXPECT_EQ(graph->FullName(), "aml");

  absl::flat_hash_set<const zetasql::GraphElementLabel*> all_graph_labels;
  ZETASQL_EXPECT_OK(graph->GetLabels(all_graph_labels));
  EXPECT_EQ(all_graph_labels.size(), 2);

  // Test labels from graph.
  {
    const zetasql::GraphElementLabel* graph_label;
    ZETASQL_EXPECT_OK(graph->FindLabelByName("GraphNode", graph_label));
    EXPECT_NE(graph_label, nullptr);
    EXPECT_EQ(graph_label->FullName(), "aml.GraphNode");

    absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>
        property_declarations_from_label;
    ZETASQL_EXPECT_OK(
        graph_label->GetPropertyDeclarations(property_declarations_from_label));
    EXPECT_EQ(property_declarations_from_label.size(), 3);
  }
  {
    const zetasql::GraphElementLabel* graph_label;
    ZETASQL_EXPECT_OK(graph->FindLabelByName("GraphEdge", graph_label));
    EXPECT_NE(graph_label, nullptr);
    EXPECT_EQ(graph_label->FullName(), "aml.GraphEdge");

    absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>
        property_declarations_from_label;
    ZETASQL_EXPECT_OK(
        graph_label->GetPropertyDeclarations(property_declarations_from_label));
    EXPECT_EQ(property_declarations_from_label.size(), 5);
  }

  // Test property declarations from graph.
  const zetasql::GraphPropertyDeclaration* property_declaration;
  ZETASQL_EXPECT_OK(graph->FindPropertyDeclarationByName("id", property_declaration));
  EXPECT_NE(property_declaration, nullptr);
  EXPECT_EQ(property_declaration->FullName(), "aml.id");
  EXPECT_EQ(property_declaration->Type(), zetasql::types::Int64Type());

  ZETASQL_EXPECT_OK(
      graph->FindPropertyDeclarationByName("label", property_declaration));
  EXPECT_NE(property_declaration, nullptr);
  EXPECT_EQ(property_declaration->FullName(), "aml.label");
  EXPECT_EQ(property_declaration->Type(), zetasql::types::StringType());

  ZETASQL_EXPECT_OK(
      graph->FindPropertyDeclarationByName("properties", property_declaration));
  EXPECT_NE(property_declaration, nullptr);
  EXPECT_EQ(property_declaration->FullName(), "aml.properties");
  EXPECT_EQ(property_declaration->Type(), zetasql::types::JsonType());

  // Test node tables from graph.
  absl::flat_hash_set<const zetasql::GraphNodeTable*> node_tables;
  ZETASQL_EXPECT_OK(graph->GetNodeTables(node_tables));
  EXPECT_EQ(node_tables.size(), 1);
  const zetasql::GraphNodeTable* node_table = *node_tables.begin();
  EXPECT_EQ(node_table->FullName(), "aml.GraphNode");
  EXPECT_TRUE(node_table->HasDynamicLabel());
  EXPECT_TRUE(node_table->HasDynamicProperties());

  // Test edge tables from graph.
  absl::flat_hash_set<const zetasql::GraphEdgeTable*> edge_tables;
  ZETASQL_EXPECT_OK(graph->GetEdgeTables(edge_tables));
  EXPECT_EQ(edge_tables.size(), 1);
  const zetasql::GraphEdgeTable* edge_table = *edge_tables.begin();
  EXPECT_EQ(edge_table->FullName(), "aml.GraphEdge");
  EXPECT_TRUE(edge_table->HasDynamicLabel());
  EXPECT_TRUE(edge_table->HasDynamicProperties());
}

TEST_F(CatalogTest, FindPropertyGraphIsNotFound) {
  const zetasql::PropertyGraph* graph;
  EXPECT_THAT(catalog().FindPropertyGraph({"BAR"}, graph, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(graph, nullptr);
}

TEST_F(CatalogTest, FindTableWithSynonym) {
  MakeCatalog({
      R"(CREATE TABLE mytable (
          int64_col INT64 NOT NULL,
          string_col STRING(MAX)
        ) PRIMARY KEY (int64_col)
      )",
      R"(ALTER TABLE mytable ADD SYNONYM syn)"});

  const zetasql::Table* mytable;
  ZETASQL_ASSERT_OK(catalog().FindTable({"mytable"}, &mytable, {}));
  ASSERT_NE(mytable, nullptr);
  EXPECT_EQ(mytable->Name(), "mytable");
  EXPECT_EQ(mytable->FullName(), "mytable");

  const zetasql::Table* syn;
  ZETASQL_ASSERT_OK(catalog().FindTable({"syn"}, &syn, {}));
  ASSERT_NE(syn, nullptr);
  EXPECT_EQ(syn->Name(), "syn");
  EXPECT_EQ(syn->FullName(), "syn");

  const QueryableTable* queryable_mytable =
      dynamic_cast<const QueryableTable*>(mytable);
  EXPECT_NE(queryable_mytable, nullptr);

  const QueryableTable* queryable_syn =
      dynamic_cast<const QueryableTable*>(syn);
  EXPECT_NE(queryable_syn, nullptr);

  EXPECT_EQ(queryable_mytable->wrapped_table(), queryable_syn->wrapped_table());
}

TEST_F(CatalogTest, CatalogGettersWithNamedSchema) {
  test::ScopedEmulatorFeatureFlagsSetter flag_setter({
      .enable_user_defined_functions = true,
  });

  MakeCatalog(
      {R"(CREATE SCHEMA mynamedschema1)", R"(CREATE SCHEMA mynamedschema2)",
       R"(CREATE TABLE mynamedschema1.mytable (int64_col INT64 NOT NULL,
        string_col STRING(MAX)) PRIMARY KEY (int64_col))",
       R"(CREATE TABLE mynamedschema2.mytable (int64_col INT64 NOT NULL,
        string_col STRING(MAX)) PRIMARY KEY (int64_col))",
       R"(CREATE VIEW mynamedschema1.myview SQL SECURITY INVOKER AS SELECT
        t.string_col AS view_col FROM mynamedschema1.mytable as t)",
       R"(CREATE FUNCTION mynamedschema1.udf_1() RETURNS INT64 SQL SECURITY INVOKER AS
        ((SELECT MAX(t.int64_col) FROM mynamedschema1.mytable as t)))"});

  // GetCatalog is a protected member of EnumerableCatalog, so it will be tested
  // through Catalog objects.
  const zetasql::Table* mytable1;
  ZETASQL_ASSERT_OK(catalog().FindTable({"mynamedschema1", "mytable"}, &mytable1, {}));
  ASSERT_NE(mytable1, nullptr);
  EXPECT_EQ(mytable1->Name(), "mytable");
  EXPECT_EQ(mytable1->FullName(), "mynamedschema1.mytable");

  const zetasql::Table* mytable2;
  ZETASQL_ASSERT_OK(catalog().FindTable({"mynamedschema2", "mytable"}, &mytable2, {}));
  ASSERT_NE(mytable2, nullptr);
  EXPECT_EQ(mytable2->Name(), "mytable");
  EXPECT_EQ(mytable2->FullName(), "mynamedschema2.mytable");

  const zetasql::Table* mytable3;
  EXPECT_THAT(catalog().FindTable({"mynamedschema3", "mytable"}, &mytable3, {}),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(mytable3, nullptr);

  const zetasql::Table* myview;
  ZETASQL_ASSERT_OK(catalog().FindTable({"mynamedschema1", "myview"}, &myview, {}));
  ASSERT_NE(myview, nullptr);
  EXPECT_EQ(myview->Name(), "myview");
  EXPECT_EQ(myview->FullName(), "mynamedschema1.myview");

  const zetasql::Function* udf_1;
  ZETASQL_ASSERT_OK(catalog().FindFunction({"mynamedschema1", "udf_1"}, &udf_1, {}));
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");
  EXPECT_EQ(udf_1->FullName(false), "mynamedschema1.udf_1");

  absl::flat_hash_set<const zetasql::Catalog*> catalogs;
  ZETASQL_ASSERT_OK(catalog().GetCatalogs(&catalogs));
  EXPECT_THAT(catalogs, Contains(Property(&zetasql::Catalog::FullName,
                                          "mynamedschema1")));
  EXPECT_THAT(catalogs, Contains(Property(&zetasql::Catalog::FullName,
                                          "mynamedschema2")));

  for (const zetasql::Catalog* catalog : catalogs) {
    if (!catalog->Is<QueryableNamedSchema>()) continue;
    QueryableNamedSchema* non_const_named_schema =
        const_cast<QueryableNamedSchema*>(
            catalog->GetAs<QueryableNamedSchema>());

    const zetasql::Table* mytable;
    ZETASQL_ASSERT_OK(non_const_named_schema->GetTable("mytable", &mytable, {}));
    ASSERT_NE(mytable, nullptr);
    EXPECT_EQ(mytable->FullName(), catalog->FullName() + ".mytable");
  }
};

TEST_F(CatalogTest, CatalogGettersWithUDFs) {
  test::ScopedEmulatorFeatureFlagsSetter flag_setter({
      .enable_user_defined_functions = true,
  });
  MakeCatalog({
      R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (x + 1))",
      R"(CREATE TABLE t1 (col1 INT64, col2 INT64 AS (udf_1(col1)))
         PRIMARY KEY (col1))",
      R"(CREATE FUNCTION STRCMP_CUSTOM(x STRING, y STRING) RETURNS
        INT64 SQL SECURITY INVOKER AS (CASE
                  WHEN x = y THEN 0
                  WHEN x > y THEN 1
                  ELSE -1
              END))",
  });

  const zetasql::Function* udf_1;
  ZETASQL_ASSERT_OK(catalog().FindFunction({"udf_1"}, &udf_1, {}));
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const zetasql::Function* strcmp_custom;
  ZETASQL_ASSERT_OK(catalog().FindFunction({"STRCMP_CUSTOM"}, &strcmp_custom, {}));
  ASSERT_NE(strcmp_custom, nullptr);
  EXPECT_EQ(strcmp_custom->Name(), "STRCMP_CUSTOM");

  const zetasql::Table* t1;
  ZETASQL_ASSERT_OK(catalog().FindTable({"t1"}, &t1, {}));
  ASSERT_NE(t1, nullptr);
  EXPECT_EQ(t1->Name(), "t1");
  EXPECT_EQ(t1->FullName(), "t1");
};

TEST_F(CatalogTest, GetExtendedTypes) {
  MakeCatalog(
      {
          R"(
        CREATE TABLE test_table (
          key bigint primary key,
          value varchar
        ))",
      },
      database_api::DatabaseDialect::POSTGRESQL);

  const zetasql::Type* int64_t;
  ZETASQL_ASSERT_OK(catalog().FindType({"INT64"}, &int64_t));
  EXPECT_EQ(int64_t, zetasql::types::Int64Type());

  const zetasql::Type* pg_numeric_type;
  ZETASQL_ASSERT_OK(catalog().FindType({"Pg", "Numeric"}, &pg_numeric_type));
  EXPECT_EQ(pg_numeric_type, GetPgNumericType());

  const zetasql::Type* pg_jsonb_type;
  ZETASQL_ASSERT_OK(catalog().FindType({"PG", "JSONB"}, &pg_jsonb_type));
  EXPECT_EQ(pg_jsonb_type, GetPgJsonbType());

  const zetasql::Type* pg_oid_type;
  ZETASQL_ASSERT_OK(catalog().FindType({"pg", "oid"}, &pg_oid_type));
  EXPECT_EQ(pg_oid_type, GetPgOidType());

  const zetasql::Type* empty_type;
  EXPECT_THAT(catalog().FindType({"UNKNOWN"}, &empty_type),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(CatalogTest, GetUuidType) {
  MakeCatalog(
      {
          R"(
        CREATE TABLE test_table (
          k UUID,
          v STRING(MAX)
        ) PRIMARY KEY (k))",
      },
      database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);
  const zetasql::Type* uuid_type;
  ZETASQL_ASSERT_OK(catalog().FindType({"UUID"}, &uuid_type));
  EXPECT_EQ(uuid_type, zetasql::types::UuidType());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
