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
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

TEST(HintRewriterTest, RewritesOnlyEmptyHintQualifier) {
  zetasql::SimpleTable table{"test_table",
                               {{"test_col", zetasql::types::StringType()}}};
  // Make a resolved AST for
  // `@{force_index=test_index} SELECT test_col FROM test_table`
  std::vector<std::unique_ptr<zetasql::ResolvedOutputColumn>> output_columns;
  output_columns.push_back(zetasql::MakeResolvedOutputColumn(
      "test_col",
      zetasql::ResolvedColumn(/*column_id=*/1,
                                zetasql::IdString::MakeGlobal("test_table"),
                                zetasql::IdString::MakeGlobal("test_col"),
                                zetasql::types::StringType())));
  const auto& statement = zetasql::MakeResolvedQueryStmt(
      std::move(output_columns), /*is_value_table=*/false,
      zetasql::MakeResolvedTableScan(
          {zetasql::ResolvedColumn(
              /*column_id=*/1, zetasql::IdString::MakeGlobal("test_table"),
              zetasql::IdString::MakeGlobal("test_col"),
              zetasql::types::StringType())},
          &table, /*for_system_time_expr=*/nullptr));
  statement->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", "force_index",
      zetasql::MakeResolvedLiteral(
          zetasql::Value::StringValue("test_index"))));
  statement->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"unknown", "force_index",
      zetasql::MakeResolvedLiteral(
          zetasql::Value::StringValue("test_index"))));

  HintRewriter rewriter;
  ZETASQL_ASSERT_OK(statement->Accept(&rewriter));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::ResolvedQueryStmt> rewritten_statement,
      rewriter.ConsumeRootNode<zetasql::ResolvedQueryStmt>());
  EXPECT_EQ(rewritten_statement->hint_list_size(), 2);
  EXPECT_EQ(rewritten_statement->hint_list(0)->qualifier(), "spanner");
  EXPECT_EQ(rewritten_statement->hint_list(1)->qualifier(), "unknown");
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
