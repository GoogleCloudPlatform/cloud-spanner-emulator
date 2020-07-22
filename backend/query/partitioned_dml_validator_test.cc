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

#include "backend/query/partitioned_dml_validator.h"

#include <memory>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/queryable_table.h"
#include "common/errors.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class PartitionedDMLValidatorTest : public testing::Test {
 public:
  PartitionedDMLValidatorTest()
      : fn_catalog_(&type_factory_),
        schema_(test::CreateSchemaWithMultiTables(&type_factory_)),
        catalog_(absl::make_unique<Catalog>(schema_.get(), &fn_catalog_,
                                            /*reader=*/nullptr)) {}

 protected:
  std::unique_ptr<const zetasql::AnalyzerOutput> AnalyzeQuery(
      const std::string& sql) {
    std::unique_ptr<const zetasql::AnalyzerOutput> output;
    ZETASQL_EXPECT_OK(zetasql::AnalyzeStatement(sql, MakeGoogleSqlAnalyzerOptions(),
                                          catalog_.get(), &type_factory_,
                                          &output));
    return output;
  }

 private:
  zetasql::TypeFactory type_factory_;

  const FunctionCatalog fn_catalog_;

  std::unique_ptr<const Schema> schema_;

  std::unique_ptr<Catalog> catalog_;
};

TEST_F(PartitionedDMLValidatorTest, PartitionedDMLOnlySupportsSimpleQuery) {
  auto stmt = AnalyzeQuery(
      "DELETE FROM test_table t1 WHERE int64_col > "
      "(SELECT MAX(int64_col) FROM test_table2 t2 WHERE "
      "t1.string_col = t2.string_col)");
  PartitionedDMLValidator validator;
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::PartitionedDMLOnlySupportsSimpleQuery());
}

TEST_F(PartitionedDMLValidatorTest, PartitionedDMLDoesNotSupportInsert) {
  auto stmt = AnalyzeQuery(
      "INSERT INTO test_table(int64_col, string_col) "
      "VALUES (1,'a')");
  PartitionedDMLValidator validator;
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::NoInsertForPartitionedDML());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
