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

#include "backend/query/hint_rewriter.h"

#include <memory>
#include <utility>
#include <vector>

#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/type.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

TEST(HintRewriterTest, RewritesOnlyEmptyHintQualifier) {
  googlesql::SimpleTable table{
      "test_table",
      {googlesql::SimpleTable::NameAndType{"test_col",
                                           googlesql::types::StringType()}}};
  // Make a resolved AST for
  // `@{force_index=test_index} SELECT test_col FROM test_table`
  std::vector<std::unique_ptr<googlesql::ResolvedOutputColumn>> output_columns;
  output_columns.push_back(googlesql::MakeResolvedOutputColumn(
      "test_col",
      googlesql::ResolvedColumn(/*column_id=*/1,
                                googlesql::IdString::MakeGlobal("test_table"),
                                googlesql::IdString::MakeGlobal("test_col"),
                                googlesql::types::StringType())));
  const auto& statement = googlesql::MakeResolvedQueryStmt(
      std::move(output_columns), /*is_value_table=*/false,
      googlesql::MakeResolvedTableScan(
          {googlesql::ResolvedColumn(
              /*column_id=*/1, googlesql::IdString::MakeGlobal("test_table"),
              googlesql::IdString::MakeGlobal("test_col"),
              googlesql::types::StringType())},
          &table, /*for_system_time_expr=*/nullptr));
  statement->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"", "force_index",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::StringValue("test_index"))));
  statement->add_hint_list(googlesql::MakeResolvedOption(
      /*qualifier=*/"unknown", "force_index",
      googlesql::MakeResolvedLiteral(
          googlesql::Value::StringValue("test_index"))));

  HintRewriter rewriter;
  GOOGLESQL_ASSERT_OK(statement->Accept(&rewriter));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<googlesql::ResolvedQueryStmt> rewritten_statement,
      rewriter.ConsumeRootNode<googlesql::ResolvedQueryStmt>());
  EXPECT_EQ(rewritten_statement->hint_list_size(), 2);
  EXPECT_EQ(rewritten_statement->hint_list(0)->qualifier(), "spanner");
  EXPECT_EQ(rewritten_statement->hint_list(1)->qualifier(), "unknown");
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
