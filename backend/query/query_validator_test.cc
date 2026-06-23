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

#include "googlesql/public/builtin_function.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/make_node_vector.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
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

using googlesql::Function;
using googlesql_base::testing::StatusIs;

class QueryValidatorTest : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }

 private:
  googlesql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithOneTable(&type_factory_);
};

TEST_F(QueryValidatorTest, ValidateUnsupportedHintReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"destroy_table",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}


TEST_F(QueryValidatorTest, ValidateScanMethodHintWithBatchValueReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"scan_method",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("batch"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, ValidateScanMethodHintWithRowValueReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"scan_method",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("row"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, ValidateScanMethodHintWithInvalidValueReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"scan_method",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("invalid"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, ValidateHintWithUnmatchedValueTypeReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, ValidateForceIndexHintWithBaseTableReturnsOK) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("_base_table"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, ValidateForceIndexHintWithExistingIndexRetunsOK) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("test_index"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest,
       ValidateForceIndexHintWithNonexistingIndexReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("buggy_index"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, ValidateIndexStrategyHintWithCorrectValueReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"index_strategy",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::String("force_index_union"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_EXPECT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest,
       ValidateIndexStrategyHintWithInvalidValueReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"index_strategy",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::String("invalid_value"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  EXPECT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, CollectEmulatorOnlyOptionsFromHints) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator",
      /*name=*/"disable_query_null_filtered_index_check",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  {
    QueryEngineOptions opts;
    QueryValidator validator{{.schema = schema()}, &opts};
    GOOGLESQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
    EXPECT_TRUE(opts.disable_query_null_filtered_index_check);
  }

  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator",
      /*name=*/"disable_query_partitionability_check",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  {
    QueryEngineOptions opts;
    QueryValidator validator{{.schema = schema()}, &opts};
    GOOGLESQL_ASSERT_OK(resolved_query_stmt->Accept(&validator));
    EXPECT_TRUE(opts.disable_query_partitionability_check);
  }
}

TEST_F(QueryValidatorTest, ValidateDisableInlineHintReturnsOK) {
  googlesql::TypeFactory type_factory;
  std::map<std::string, std::unique_ptr<googlesql::Function>> functions;
  googlesql::GoogleSQLBuiltinFunctionOptions options;

  googlesql::GetGoogleSQLFunctions(&type_factory, options, &functions);

  googlesql::Function* substr = functions["substr"].get();
  const googlesql::FunctionSignature* signature = substr->GetSignature(0);

  std::unique_ptr<googlesql::ResolvedFunctionCall> resolved_function_call =
      googlesql::MakeResolvedFunctionCall(
          googlesql::types::StringType(), substr, *signature,
          googlesql::MakeNodeVectorP<const googlesql::ResolvedExpr>(
              googlesql::MakeResolvedLiteral(googlesql::Value::String("Hello")),
              googlesql::MakeResolvedLiteral(googlesql::Value::Int32(0)),
              googlesql::MakeResolvedLiteral(googlesql::Value::Int32(1))),
          googlesql::ResolvedFunctionCall::DEFAULT_ERROR_MODE);

  resolved_function_call->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"disable_inline",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_function_call->Accept(&validator));
}

TEST_F(QueryValidatorTest, HashJoinExecutionHintOnePassReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = googlesql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"hash_join_execution",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("one_pass"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_join_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, HashJoinExecutionHintMultiPassReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = googlesql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"hash_join_execution",
      googlesql::MakeResolvedLiteral(googlesql::Value::String("multi_pass"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_join_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, HashJoinExecutionHintInvalidReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = googlesql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"hash_join_execution",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::String("invalid_value"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_join_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest,
       ValidateJoinMethodHintPushBroadcastHashJoinReturnsOk) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = googlesql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"join_method",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::String("push_broadcast_hash_join"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_EXPECT_OK(resolved_join_scan->Accept(&validator));
}

TEST_F(QueryValidatorTest, ValidateJoinMethodHintInvalidValueReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  auto resolved_join_scan = googlesql::MakeResolvedJoinScan();
  resolved_join_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"join_method",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::String("invalid_value"))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  EXPECT_THAT(resolved_join_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// Tests for ignore_unknown_hints
TEST_F(QueryValidatorTest, IgnoreUnknownHints_SpannerQualifierFails) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  // Using spanner qualifier for ignore_unknown_hints should fail
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_EmptyQualifierFails) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  // Using empty qualifier for ignore_unknown_hints should fail
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_EnabledSucceedsWithUnknownHint) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"unknown_hint",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_query_stmt->Accept(&validator));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_AppliesToChildNodes) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"unknown_hint",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  GOOGLESQL_ASSERT_OK(resolved_query_stmt->Accept(&validator));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_DisabledAppliesToChildNodes) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"unknown_hint",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(false))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_InvalidValueTypeFails) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Int64(1))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_KnownHintInvalidValueStillFails) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(resolved_table_scan));
  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest,
       IgnoreUnknownHints_InSubqueryDoesNotLeakToRootQuery) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);

  std::unique_ptr<googlesql::ResolvedSubqueryExpr> subquery_expr =
      googlesql::MakeResolvedSubqueryExpr();
  subquery_expr->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  std::unique_ptr<googlesql::ResolvedFilterScan> filter_scan =
      googlesql::MakeResolvedFilterScan(
          /*column_list=*/{}, std::move(resolved_table_scan),
          /*filter_expr=*/std::move(subquery_expr));

  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(filter_scan));

  resolved_query_stmt->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"unknown_hint",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryValidatorTest, IgnoreUnknownHints_InSubqueryIsInvalid) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<googlesql::ResolvedTableScan> resolved_table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);

  std::unique_ptr<googlesql::ResolvedSubqueryExpr> subquery_expr =
      googlesql::MakeResolvedSubqueryExpr();
  subquery_expr->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"spanner_emulator", /*name=*/"ignore_unknown_hints",
      googlesql::MakeResolvedLiteral(googlesql::Value::Bool(true))));

  std::unique_ptr<googlesql::ResolvedFilterScan> filter_scan =
      googlesql::MakeResolvedFilterScan(
          /*column_list=*/{}, std::move(resolved_table_scan),
          /*filter_expr=*/std::move(subquery_expr));

  std::unique_ptr<googlesql::ResolvedQueryStmt> resolved_query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(filter_scan));

  QueryEngineOptions opts;
  QueryValidator validator{{.schema = schema()}, &opts};
  ASSERT_THAT(resolved_query_stmt->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
