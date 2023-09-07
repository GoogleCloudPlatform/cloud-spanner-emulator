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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/function.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using ::testing::Contains;
using ::testing::Property;
using ::zetasql_base::testing::StatusIs;

// An integration test that uses Catalog to call zetasql::AnalyzeStatement.
class AnalyzeStatementTest : public testing::Test {
 protected:
  absl::Status AnalyzeStatement(absl::string_view sql) {
    zetasql::TypeFactory type_factory{};
    std::unique_ptr<const Schema> schema =
        test::CreateSchemaWithOneTable(&type_factory);
    FunctionCatalog function_catalog{&type_factory};
    auto analyzer_options = MakeGoogleSqlAnalyzerOptions();
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

TEST_F(AnalyzeStatementTest, SelectFromPGInformationSchema) {
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "SELECT column_name FROM pg_information_schema.columns"));
}

TEST_F(AnalyzeStatementTest, SelectFromPGCatalog) {
  ZETASQL_EXPECT_OK(AnalyzeStatement("SELECT tablename FROM pg_catalog.pg_tables"));
}

class CatalogTest : public testing::Test {
 public:
  CatalogTest() : type_factory_(), function_catalog_(&type_factory_) {}

  void SetUp() override {
    schema_ = test::CreateSchemaWithOneTable(&type_factory_);
    catalog_ = std::make_unique<Catalog>(schema_.get(), &function_catalog_,
                                         &type_factory_);
  }

  void MakeCatalog(absl::Span<const std::string> statements) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema_,
                         test::CreateSchemaFromDDL(statements, &type_factory_));
    catalog_ = std::make_unique<Catalog>(schema_.get(), &function_catalog_,
                                         &type_factory_);
  }

  void MakeChangeStreamInternalQueryCatalog(
      absl::Span<const std::string> statements) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema_,
                         test::CreateSchemaFromDDL(statements, &type_factory_));
    catalog_ = std::make_unique<Catalog>(schema_.get(), &function_catalog_,
                                         &type_factory_);
    change_stream_internal_query_catalog_ = std::make_unique<Catalog>(
        schema_.get(), &function_catalog_, &type_factory_,
        MakeGoogleSqlAnalyzerOptions(), /*reader=*/nullptr,
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
  FunctionCatalog function_catalog_;
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

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
