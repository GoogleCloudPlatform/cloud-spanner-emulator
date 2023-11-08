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

#include <memory>
#include <string>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/change_stream/queryable_change_stream_tvf.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/schema.h"
#include "tests/common/schema_constructor.h"
#include "third_party/spanner_pg/interface/emulator_parser.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "zetasql/base/status_macros.h"
namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {
class PgQueryableChangeStreamTvfTest : public testing::Test {
 public:
  PgQueryableChangeStreamTvfTest()
      : schema_(test::CreateSchemaWithOneTableAndOneChangeStream(
            &type_factory_, database_api::DatabaseDialect::POSTGRESQL)),
        analyzer_options_(MakeGoogleSqlAnalyzerOptions()),
        fn_catalog_(&type_factory_),
        catalog_(std::make_unique<Catalog>(
            schema_.get(), &fn_catalog_, &type_factory_, analyzer_options_)) {}

 protected:
  // TODO: Add unit tests for change streams in PG once TVF can be
  // analyzed externally in pg.
  absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
  AnalyzePGStatement(const std::string& sql) {
    analyzer_options_.CreateDefaultArenasIfNotSet();
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
    return postgres_translator::spangres::ParseAndAnalyzePostgreSQL(
        sql, catalog_.get(), analyzer_options_, &type_factory_);
  }
  std::unique_ptr<const Schema> schema_ = nullptr;
  zetasql::TypeFactory type_factory_;
  zetasql::AnalyzerOptions analyzer_options_;
  const FunctionCatalog fn_catalog_;
  std::unique_ptr<Catalog> catalog_;
};

TEST_F(PgQueryableChangeStreamTvfTest, CreateChangeStreamTvfPgOk) {
  const auto* schema_change_stream =
      schema_->FindChangeStream("change_stream_test_table");
  ASSERT_NE(schema_change_stream, nullptr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto queryable_tvf,
                       QueryableChangeStreamTvf::Create(
                           schema_change_stream->tvf_name(), analyzer_options_,
                           catalog_.get(), &type_factory_, true));
  EXPECT_EQ(queryable_tvf->Name(), "read_json_change_stream_test_table");
  EXPECT_EQ(queryable_tvf->result_schema().num_columns(), 1);
  EXPECT_EQ(queryable_tvf->result_schema().column(0).type->DebugString(),
            "PG.JSONB");
  EXPECT_EQ(queryable_tvf->GetSignature(0)->arguments().size(), 5);
}

TEST_F(PgQueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryPositionalArg) {
  ZETASQL_EXPECT_OK(AnalyzePGStatement(
      "SELECT * FROM "
      "spanner.read_json_change_stream_test_table ("
      "'2022-09-27T12:30:00.123456Z'::timestamptz,"
      "NULL::timestamptz, NULL::text, 1000 , NULL::text[] )"));
}

TEST_F(PgQueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryNamedArgFail) {
  EXPECT_THAT(
      AnalyzePGStatement(
          "SELECT * FROM "
          "spanner.read_json_change_stream_test_table ("
          "start_timestamp=>'2022-09-27T12:30:00.123456Z'::timestamptz,"
          "end_timestamp=>NULL::timestamptz, partition_token=>NULL::text, "
          "heartbeat_milliseconds=>1000 , "
          "read_options=>NULL::text[] "
          ")"),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kUnimplemented,
          testing::HasSubstr("Named arguments are not supported")));
}

TEST_F(PgQueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryWrongArgTypeFail) {
  EXPECT_THAT(
      AnalyzePGStatement(
          "SELECT * FROM "
          "spanner.read_json_change_stream_test_table ("
          "'2022-09-27T12:30:00.123456Z'::timestamptz,"
          "NULL::timestamptz, NULL::timestamptz, 1000 , NULL::text[] )"),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kNotFound,
          testing::HasSubstr(
              "No function matches the given name and argument types.")));
}

TEST_F(PgQueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryWrongArgNumFail) {
  EXPECT_THAT(
      AnalyzePGStatement("SELECT * FROM "
                         "spanner.read_json_change_stream_test_table ("
                         "'2022-09-27T12:30:00.123456Z'::timestamptz,"
                         "NULL::timestamptz, NULL::text[] )"),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kNotFound,
          testing::HasSubstr(
              "No function matches the given name and argument types.")));
}

TEST_F(PgQueryableChangeStreamTvfTest,
       AnalyzeChangeStreamTvfQueryWrongTvfNameFail) {
  EXPECT_THAT(
      AnalyzePGStatement(
          "SELECT * FROM spanner.read_json_null ("
          "'2022-09-27T12:30:00.123456Z'::timestamptz,"
          "NULL::timestamptz, NULL::text, 1000 , NULL::text[] )"),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kNotFound,
          testing::HasSubstr(
              "No function matches the given name and argument types.")));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
