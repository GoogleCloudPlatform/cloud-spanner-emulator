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

#include "backend/query/query_validator.h"

#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/query_engine_options.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql_base::testing::StatusIs;

class QueryValidatorTest : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }

 private:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithOneTable(&type_factory_);
};

TEST_F(QueryValidatorTest, ValidateUnsupportedHintReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"destroy_table",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{schema(), &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, ValidateHintWithUnmatchedValueTypeReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{schema(), &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, ValidateForceIndexHintWithBaseTableReturnsOK) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("_base_table"))));

  QueryEngineOptions opts;
  QueryValidator validator{schema(), &opts};
  ZETASQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, ValidateForceIndexHintWithExistingIndexRetunsOK) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("test_index"))));

  QueryEngineOptions opts;
  QueryValidator validator{schema(), &opts};
  ZETASQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest,
       ValidateForceIndexHintWithNonexistingIndexReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("buggy_index"))));

  QueryEngineOptions opts;
  QueryValidator validator{schema(), &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, CollectEmulatorOnlyOptionsFromHints) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator",
      /*name=*/"disable_query_null_filtered_index_check",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  {
    QueryEngineOptions opts;
    QueryValidator validator{schema(), &opts};
    ZETASQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
    EXPECT_TRUE(opts.disable_query_null_filtered_index_check);
  }

  std::unique_ptr<zetasql::ResolvedQueryStmt> resolved_query_stmt =
      zetasql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator",
      /*name=*/"disable_query_partitionability_check",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  {
    QueryEngineOptions opts;
    QueryValidator validator{schema(), &opts};
    ZETASQL_ASSERT_OK(resolved_query_stmt->Accept(&validator));
    EXPECT_TRUE(opts.disable_query_partitionability_check);
  }
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
