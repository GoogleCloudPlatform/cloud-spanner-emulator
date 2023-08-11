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

#include "backend/query/change_stream/queryable_change_stream_tvf.h"

#include <memory>

#include "zetasql/public/analyzer_options.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {
class QueryableChangeStreamTvfTest : public testing::Test {
 public:
  QueryableChangeStreamTvfTest()
      : schema_(
            test::CreateSchemaWithOneTableAndOneChangeStream(&type_factory_)),
        analyzer_options_(MakeGoogleSqlAnalyzerOptions()),
        fn_catalog_(&type_factory_),
        catalog_(std::make_unique<Catalog>(
            schema_.get(), &fn_catalog_, &type_factory_, analyzer_options_)) {}

 protected:
  absl::Status AnalyzeStatement(absl::string_view sql) {
    std::unique_ptr<const zetasql::AnalyzerOutput> output{};
    return zetasql::AnalyzeStatement(sql, analyzer_options_, catalog_.get(),
                                       &type_factory_, &output);
  }
  std::unique_ptr<const Schema> schema_ = nullptr;
  zetasql::TypeFactory type_factory_;
  zetasql::AnalyzerOptions analyzer_options_;
  const FunctionCatalog fn_catalog_;
  std::unique_ptr<Catalog> catalog_;
};

TEST_F(QueryableChangeStreamTvfTest, CreateChangeStreamTvfOk) {
  const auto* schema_change_stream =
      schema_->FindChangeStream("change_stream_test_table");
  ASSERT_NE(schema_change_stream, nullptr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto queryable_tvf,
      QueryableChangeStreamTvf::Create(schema_change_stream, analyzer_options_,
                                       catalog_.get(), &type_factory_));
  EXPECT_EQ(queryable_tvf->Name(), "READ_change_stream_test_table");
  EXPECT_EQ(queryable_tvf->result_schema().num_columns(), 1);
  EXPECT_EQ(queryable_tvf->GetSignature(0)->arguments().size(), 5);
}

TEST_F(QueryableChangeStreamTvfTest, AnalyzeChangeStreamTvfQueryPositionalArg) {
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "SELECT * FROM "
      "READ_change_stream_test_table ( '2022-09-27T12:30:00.123456Z', "
      "NULL, NULL, 1000 )"));
}

TEST_F(QueryableChangeStreamTvfTest, AnalyzeChangeStreamTvfQueryNamedArg) {
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "SELECT ChangeRecord FROM READ_change_stream_test_table ( "
      "start_timestamp=>'2022-09-27T12:30:00.123456Z', end_timestamp=>NULL, "
      "partition_token=>NULL, heartbeat_milliseconds=>1000 )"));
}

TEST_F(QueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryWrongArgTypeFail) {
  absl::Status status = AnalyzeStatement(
      "SELECT * FROM "
      "READ_change_stream_test_table ( 'not_a_timestamp', "
      "NULL, NULL, 1000 )");
  EXPECT_THAT(
      status,
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr(
              "Could not cast literal \"not_a_timestamp\" to type TIMESTAMP")));
}

TEST_F(QueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryWrongArgNumFail) {
  absl::Status status = AnalyzeStatement(
      "SELECT * FROM "
      "READ_change_stream_test_table ( '2022-09-27T12:30:00.123456Z', "
      " NULL, 1000 )");
  EXPECT_THAT(
      status,
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr(
              "No matching signature for READ_change_stream_test_table")));
}

TEST_F(QueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryWrongTvfNameFail) {
  absl::Status status = AnalyzeStatement(
      "SELECT * FROM "
      "READ_change_stream_null_table ( '2022-09-27T12:30:00.123456Z', "
      " NULL, NULL, 1000 )");
  EXPECT_THAT(status, zetasql_base::testing::StatusIs(
                          absl::StatusCode::kInvalidArgument,
                          testing::HasSubstr("Table-valued function not found: "
                                             "READ_change_stream_null_table")));
}

TEST_F(QueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryPositionWrongArgNameFail) {
  absl::Status status = AnalyzeStatement(
      "SELECT ChangeRecord FROM READ_change_stream_test_table ( "
      "start_timestamp=>'2022-09-27T12:30:00.123456Z', end_timestamp=>NULL, "
      "partition_string=>NULL, heartbeat_milliseconds=>1000 )");
  EXPECT_THAT(status,
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(
                      "Named argument partition_string not found in signature "
                      "for call to function READ_change_stream_test_table")));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
