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

#include "backend/query/partitionability_validator.h"

#include <memory>
#include <utility>
#include <vector>

#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output.h"
#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/query_engine.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/table.h"
#include "common/feature_flags.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class PartitionabilityValidatorTest : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }
  googlesql::TypeFactory* type_factory() { return &type_factory_; }

 private:
  googlesql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithMultiTables(&type_factory_);
};

TEST_F(PartitionabilityValidatorTest, ValidateSimpleScanPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const googlesql::ResolvedScan> table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<const googlesql::ResolvedScan> project_scan =
      googlesql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/{},
                                         std::move(table_scan));
  std::unique_ptr<const googlesql::ResolvedQueryStmt> query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  GOOGLESQL_ASSERT_OK(query_stmt->Accept(&validator));
}

TEST_F(PartitionabilityValidatorTest,
       ValidateSimpleScanWithFilterPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const googlesql::ResolvedScan> table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<const googlesql::ResolvedScan> filter_scan =
      googlesql::MakeResolvedFilterScan(/*column_list=*/{},
                                        std::move(table_scan),
                                        /*filter_expr=*/nullptr);
  std::unique_ptr<const googlesql::ResolvedScan> project_scan =
      googlesql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/{},
                                         std::move(filter_scan));
  std::unique_ptr<const googlesql::ResolvedQueryStmt> query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  GOOGLESQL_ASSERT_OK(query_stmt->Accept(&validator));
}

TEST_F(PartitionabilityValidatorTest, ValidateSubqueryColumnNonPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const googlesql::ResolvedScan> table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  // set up a computed column with subquery expr.
  std::unique_ptr<const googlesql::ResolvedExpr> subquery_expr =
      googlesql::MakeResolvedSubqueryExpr();
  googlesql::ResolvedColumn column{
      /*column_id=*/1, googlesql::IdString::MakeGlobal("table_name"),
      googlesql::IdString::MakeGlobal("col_name"),
      type_factory()->get_string()};
  std::unique_ptr<const googlesql::ResolvedComputedColumn> expr =
      googlesql::MakeResolvedComputedColumn(column, std::move(subquery_expr));

  std::vector<std::unique_ptr<const googlesql::ResolvedComputedColumn>>
      expr_list;
  expr_list.push_back(std::move(expr));
  // project scan has a subquery expr column.
  std::unique_ptr<const googlesql::ResolvedScan> project_scan =
      googlesql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/std::move(expr_list),
                                         std::move(table_scan));
  std::unique_ptr<const googlesql::ResolvedQueryStmt> query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ASSERT_THAT(query_stmt->Accept(&validator),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionabilityValidatorTest,
       ValidateFilterWithSubqueryNonPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const googlesql::ResolvedScan> table_scan =
      googlesql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<const googlesql::ResolvedExpr> subquery_expr =
      googlesql::MakeResolvedSubqueryExpr();
  std::unique_ptr<const googlesql::ResolvedScan> filter_scan =
      googlesql::MakeResolvedFilterScan(
          /*column_list=*/{}, std::move(table_scan),
          /*filter_expr=*/std::move(subquery_expr));
  std::unique_ptr<const googlesql::ResolvedScan> project_scan =
      googlesql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/{},
                                         std::move(filter_scan));
  std::unique_ptr<const googlesql::ResolvedQueryStmt> query_stmt =
      googlesql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ASSERT_THAT(query_stmt->Accept(&validator),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionabilityValidatorTest, SelectOneIsPartitionable) {
  test::ScopedEmulatorFeatureFlagsSetter feature_flag_setter(
      EmulatorFeatureFlags::Flags{.enable_batch_query_with_no_table_scan =
                                      true});
  PartitionabilityValidator validator{schema()};

  std::unique_ptr<const googlesql::AnalyzerOutput> analyzer_output;
  googlesql::SimpleCatalog catalog("test simple catalog");
  googlesql::TypeFactory type_factory;
  GOOGLESQL_ASSERT_OK(googlesql::AnalyzeStatement("SELECT 1",
                                        googlesql::AnalyzerOptions(), &catalog,
                                        &type_factory, &analyzer_output));

  const googlesql::ResolvedStatement* resolved_statement =
      analyzer_output->resolved_statement();
  GOOGLESQL_EXPECT_OK(resolved_statement->Accept(&validator));
}

TEST_F(PartitionabilityValidatorTest,
       SelectConstValuesWithSortIsPartitionable) {
  test::ScopedEmulatorFeatureFlagsSetter feature_flag_setter(
      EmulatorFeatureFlags::Flags{.enable_batch_query_with_no_table_scan =
                                      true});
  PartitionabilityValidator validator{schema()};

  std::unique_ptr<const googlesql::AnalyzerOutput> analyzer_output;
  googlesql::SimpleCatalog catalog("test simple catalog");
  googlesql::TypeFactory type_factory;
  GOOGLESQL_ASSERT_OK(googlesql::AnalyzeStatement(
      "SELECT a FROM UNNEST([3, 2, 1]) AS a ORDER BY a",
      googlesql::AnalyzerOptions(), &catalog, &type_factory, &analyzer_output));

  const googlesql::ResolvedStatement* resolved_statement =
      analyzer_output->resolved_statement();
  GOOGLESQL_EXPECT_OK(resolved_statement->Accept(&validator));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
