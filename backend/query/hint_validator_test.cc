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

#include "backend/query/hint_validator.h"

#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/queryable_table.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql_base::testing::StatusIs;

class HintValidatorTest : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }

 private:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithOneTable(&type_factory_);
};

TEST_F(HintValidatorTest, ValidateUnsupportedHintReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"destroy_table",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  HintValidator validator{schema()};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(HintValidatorTest, ValidateHintWithUnmatchedValueTypeReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::Bool(true))));

  HintValidator validator{schema()};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(HintValidatorTest, ValidateForceIndexHintWithBaseTableReturnsOK) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("_base_table"))));

  HintValidator validator{schema()};
  ZETASQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(HintValidatorTest, ValidateForceIndexHintWithExistingIndexRetunsOK) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("test_index"))));

  HintValidator validator{schema()};
  ZETASQL_ASSERT_OK(resolved_table_scan->Accept(&validator));
}

TEST_F(HintValidatorTest,
       ValidateForceIndexHintWithNonexistingIndexReturnsError) {
  QueryableTable table{schema()->FindTable("test_table"), /*reader=*/nullptr};
  std::unique_ptr<zetasql::ResolvedTableScan> resolved_table_scan =
      zetasql::MakeResolvedTableScan(/*column_list=*/{}, &table,
                                       /*for_system_time_expr=*/nullptr);
  resolved_table_scan->add_hint_list(zetasql::MakeResolvedOption(
      /*qualifier=*/"", /*name=*/"force_index",
      zetasql::MakeResolvedLiteral(zetasql::Value::String("buggy_index"))));

  HintValidator validator{schema()};
  ASSERT_THAT(resolved_table_scan->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
