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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/make_node_vector.h"
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

using zetasql::Function;
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
  QueryValidator validator{{.schema = schema()}, &opts};
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
  QueryValidator validator{{.schema = schema()}, &opts};
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
  QueryValidator validator{{.schema = schema()}, &opts};
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
  QueryValidator validator{{.schema = schema()}, &opts};
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
  QueryValidator validator{{.schema = schema()}, &opts};
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
    QueryValidator validator{{.schema = schema()}, &opts};
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
    QueryValidator validator{{.schema = schema()}, &opts};
    ZETASQL_ASSERT_OK(resolved_query_stmt->Accept(&validator));
    EXPECT_TRUE(opts.disable_query_partitionability_check);
  }
}

TEST_F(QueryValidatorTest, ValidateDisableInlineHintReturnsOK) {
  zetasql::TypeFactory type_factory;
  std::map<std::string, std::unique_ptr<zetasql::Function>> functions;
  zetasql::ZetaSQLBuiltinFunctionOptions options;

  zetasql::GetZetaSQLFunctions(&type_factory, options, &functions);

  zetasql::Function* substr = functions["substr"].get();
  const zetasql::FunctionSignature* signature = substr->GetSignature(0);

  std::unique_ptr<zetasql::ResolvedFunctionCall> resolved_function_call =
      zetasql::MakeResolvedFunctionCall(
          zetasql::types::StringType(), substr, *signature,
          zetasql::MakeNodeVectorP<const zetasql::ResolvedExpr>(
              zetasql::MakeResolvedLiteral(zetasql::Value::String("Hello")),
              zetasql::MakeResolvedLiteral(zetasql::Value::Int32(0)),
              zetasql::MakeResolvedLiteral(zetasql::Value::Int32(1))),
          zetasql::ResolvedFunctionCall::DEFAULT_ERROR_MODE);

  resolved_function_call->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"disable_inline",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ZETASQL_ASSERT_OK(resolved_function_call->Accept(&validator));
}

TEST_F(QueryValidatorTest, HashJoinExecutionHintOnePassReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = zetasql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"hash_join_execution",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("one_pass"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ZETASQL_ASSERT_OK(resolved_join_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, HashJoinExecutionHintMultiPassReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = zetasql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"hash_join_execution",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("multi_pass"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ZETASQL_ASSERT_OK(resolved_join_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, HashJoinExecutionHintInvalidReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = zetasql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"hash_join_execution",
      zetasql::MakeResolvedLiteral(
          zetasql::Value::String("invalid_value"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_join_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
