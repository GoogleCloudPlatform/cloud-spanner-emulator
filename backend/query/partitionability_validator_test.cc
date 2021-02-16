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

#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/query_engine.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql_base::testing::StatusIs;

class PartitionabilityValidatorTest : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }
  zetasql::TypeFactory* type_factory() { return &type_factory_; }

 private:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithMultiTables(&type_factory_);
};

TEST_F(PartitionabilityValidatorTest, ValidateSimpleScanPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const zetasql::ResolvedScan> table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<const zetasql::ResolvedScan> project_scan =
      zetasql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/{},
                                         std::move(table_scan));
  std::unique_ptr<const zetasql::ResolvedQueryStmt> query_stmt =
      zetasql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ZETASQL_ASSERT_OK(query_stmt->Accept(&validator));
}

TEST_F(PartitionabilityValidatorTest,
       ValidateSimpleScanWithFilterPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const zetasql::ResolvedScan> table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<const zetasql::ResolvedScan> filter_scan =
      zetasql::MakeResolvedFilterScan(/*column_list=*/{},
                                        std::move(table_scan),
                                        /*filter_expr=*/nullptr);
  std::unique_ptr<const zetasql::ResolvedScan> project_scan =
      zetasql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/{},
                                         std::move(filter_scan));
  std::unique_ptr<const zetasql::ResolvedQueryStmt> query_stmt =
      zetasql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ZETASQL_ASSERT_OK(query_stmt->Accept(&validator));
}

TEST_F(PartitionabilityValidatorTest, ValidateSubqueryColumnNonPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const zetasql::ResolvedScan> table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  // set up a computed column with subquery expr.
  std::unique_ptr<const zetasql::ResolvedExpr> subquery_expr =
      zetasql::MakeResolvedSubqueryExpr();
  zetasql::ResolvedColumn column{
      /*column_id=*/1, zetasql::IdString::MakeGlobal("table_name"),
      zetasql::IdString::MakeGlobal("col_name"),
      type_factory()->get_string()};
  std::unique_ptr<const zetasql::ResolvedComputedColumn> expr =
      zetasql::MakeResolvedComputedColumn(column, std::move(subquery_expr));

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      expr_list;
  expr_list.push_back(std::move(expr));
  // project scan has a subquery expr column.
  std::unique_ptr<const zetasql::ResolvedScan> project_scan =
      zetasql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/std::move(expr_list),
                                         std::move(table_scan));
  std::unique_ptr<const zetasql::ResolvedQueryStmt> query_stmt =
      zetasql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ASSERT_THAT(query_stmt->Accept(&validator),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionabilityValidatorTest,
       ValidateFilterWithSubqueryNonPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const zetasql::ResolvedScan> table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  std::unique_ptr<const zetasql::ResolvedExpr> subquery_expr =
      zetasql::MakeResolvedSubqueryExpr();
  std::unique_ptr<const zetasql::ResolvedScan> filter_scan =
      zetasql::MakeResolvedFilterScan(
          /*column_list=*/{}, std::move(table_scan),
          /*filter_expr=*/std::move(subquery_expr));
  std::unique_ptr<const zetasql::ResolvedScan> project_scan =
      zetasql::MakeResolvedProjectScan(/*column_list=*/{},
                                         /*expr_list=*/{},
                                         std::move(filter_scan));
  std::unique_ptr<const zetasql::ResolvedQueryStmt> query_stmt =
      zetasql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ASSERT_THAT(query_stmt->Accept(&validator),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionabilityValidatorTest, ValidateEmptyQueryNotPartitionable) {
  PartitionabilityValidator validator{schema()};
  std::unique_ptr<const zetasql::ResolvedQueryStmt> query_stmt =
      zetasql::MakeResolvedQueryStmt();

  ASSERT_THAT(query_stmt->Accept(&validator),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionabilityValidatorTest, ValidateNoTableScanNonPartitionable) {
  PartitionabilityValidator validator{schema()};
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};

  std::unique_ptr<const zetasql::ResolvedScan> project_scan =
      zetasql::MakeResolvedProjectScan();
  std::unique_ptr<const zetasql::ResolvedQueryStmt> query_stmt =
      zetasql::MakeResolvedQueryStmt(/*output_column_list=*/{},
                                       /*is_value_table=*/false,
                                       std::move(project_scan));

  ASSERT_THAT(query_stmt->Accept(&validator),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
