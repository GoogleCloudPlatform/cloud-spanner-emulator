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
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"

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
    Catalog catalog{schema.get(), &function_catalog};
    std::unique_ptr<const zetasql::AnalyzerOutput> output{};
    return zetasql::AnalyzeStatement(sql, /*options_in=*/{}, &catalog,
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

TEST_F(AnalyzeStatementTest, SelectNestedCatalogFunctions) {
  ZETASQL_EXPECT_OK(
      AnalyzeStatement("SELECT "
                       "NET.IPV4_TO_INT64(b\"\\x00\\x00\\x00\\x00\")"));
}

class CatalogTest : public testing::Test {
 public:
  zetasql::EnumerableCatalog& catalog() { return catalog_; }

 private:
  zetasql::TypeFactory type_factory_{};
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithOneTable(&type_factory_);
  FunctionCatalog function_catalog_{&type_factory_};
  Catalog catalog_{schema_.get(), &function_catalog_};
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

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
