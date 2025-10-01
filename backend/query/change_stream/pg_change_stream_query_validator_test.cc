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
#include <utility>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/change_stream/change_stream_query_validator.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/schema.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
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

using ::google::spanner::emulator::backend::
    kCloudSpannerEmulatorFunctionCatalogName;
using zetasql_base::testing::StatusIs;
namespace database_api = ::google::spanner::admin::database::v1;

class PgChangeStreamQueryValidatorTest : public testing::Test {
 public:
  friend class Catalog;
  PgChangeStreamQueryValidatorTest()
      : analyzer_options_(MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone)),
        fn_catalog_(&type_factory_),
        schema_(test::CreateSchemaWithOneTableAndOneChangeStream(
            &type_factory_, database_api::DatabaseDialect::POSTGRESQL)),
        catalog_(std::make_unique<Catalog>(
            schema_.get(), &fn_catalog_, &type_factory_, analyzer_options_)) {}

 protected:
  absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
  AnalyzePGQuery(const std::string& sql) {
    analyzer_options_.CreateDefaultArenasIfNotSet();
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
    return postgres_translator::spangres::ParseAndAnalyzePostgreSQL(
        sql, catalog_.get(), analyzer_options_, &type_factory_,
        std::make_unique<FunctionCatalog>(
            &type_factory_, kCloudSpannerEmulatorFunctionCatalogName,
            schema_.get()));
  }

  zetasql::TypeFactory type_factory_;

  zetasql::AnalyzerOptions analyzer_options_;

  const FunctionCatalog fn_catalog_;

  std::unique_ptr<const Schema> schema_;

  std::unique_ptr<Catalog> catalog_;

  absl::flat_hash_map<std::string, zetasql::Value> params_;
};

TEST_F(PgChangeStreamQueryValidatorTest, ValidateArgumentLiteralsValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt, AnalyzePGQuery(absl::Substitute(
                     "SELECT * FROM "
                     "spanner.read_json_change_stream_test_table ("
                     "'$0'::timestamptz,"
                     "'$1'::timestamptz, null::text, 1000 , null::text[] )",
                     absl::Now(), absl::Now() + absl::Minutes(1))));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ZETASQL_ASSERT_OK(stmt->resolved_statement()->Accept(&validator));
}

TEST_F(PgChangeStreamQueryValidatorTest, ValidateArgumentParametersValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt, AnalyzePGQuery("SELECT * FROM "
                                "spanner.read_json_change_stream_test_table "
                                "($1::timestamptz, "
                                "$2::timestamptz, $3::text, "
                                "$4, null::text[] )"));
  params_.insert(
      {"p1", zetasql::Value::Timestamp(absl::Now() + absl::Minutes(1))});
  params_.insert(
      {"p2", zetasql::Value::Timestamp(absl::Now() + absl::Minutes(1))});
  params_.insert({"p3", zetasql::Value::String("test_token")});
  params_.insert({"p4", zetasql::Value::Int64(1000)});
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ZETASQL_ASSERT_OK(stmt->resolved_statement()->Accept(&validator));
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateMetadataCreatedForValidChangeStreamQuery) {
  absl::Time start_time = absl::Now();
  absl::Time end_time = start_time + absl::Minutes(1);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto stmt,
                       AnalyzePGQuery(absl::Substitute(
                           "SELECT * FROM "
                           "spanner.read_json_change_stream_test_table "
                           "('$0'::timestamptz, "
                           "'$1'::timestamptz, 'test_token'::text, "
                           "1000, null::text[] )",
                           start_time, end_time)));
  ChangeStreamQueryValidator validator{schema_.get(), absl::Now(),
                                       std::move(params_)};
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ZETASQL_ASSERT_OK(stmt->resolved_statement()->Accept(&validator));
  EXPECT_EQ(validator.change_stream_metadata().change_stream_name,
            "change_stream_test_table");
  EXPECT_EQ(validator.change_stream_metadata().heartbeat_milliseconds, 1000);
  EXPECT_EQ(validator.change_stream_metadata().partition_token.value(),
            "test_token");
  // absl::Time and timestamptz have different precision with timestamptz in pg
  // dialect, thus we convert them both to microseconds precision.
  EXPECT_EQ(absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ",
                             validator.change_stream_metadata().start_timestamp,
                             absl::LocalTimeZone()),
            absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ", start_time,
                             absl::LocalTimeZone()));
  EXPECT_EQ(
      absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ",
                       validator.change_stream_metadata().end_timestamp.value(),
                       absl::LocalTimeZone()),
      absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ", end_time,
                       absl::LocalTimeZone()));
  EXPECT_EQ(validator.change_stream_metadata().tvf_name,
            "read_json_change_stream_test_table");
  EXPECT_EQ(validator.change_stream_metadata().partition_table,
            "_change_stream_partition_change_stream_test_table");
  EXPECT_EQ(validator.change_stream_metadata().data_table,
            "_change_stream_data_change_stream_test_table");
  ASSERT_TRUE(validator.change_stream_metadata().is_pg);
  ASSERT_TRUE(validator.change_stream_metadata().is_change_stream_query);
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateMetadataCreatedForValidRegularQuery) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto stmt, AnalyzePGQuery("SELECT * FROM test_table"));
  ChangeStreamQueryValidator validator{schema_.get(), absl::Now(),
                                       std::move(params_)};
  ASSERT_FALSE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ASSERT_FALSE(validator.change_stream_metadata().is_change_stream_query);
}

TEST_F(PgChangeStreamQueryValidatorTest, ValidateNullStartTimestampNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt,
      AnalyzePGQuery("SELECT * FROM "
                     "spanner.read_json_change_stream_test_table ("
                     "null::timestamptz,"
                     "null::timestamptz, null::text, 1000 , null::text[] )"));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentNullStartTimestamp());
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateStartTimestampTooFarInFutureNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt, AnalyzePGQuery(absl::Substitute(
                     "SELECT * FROM "
                     "spanner.read_json_change_stream_test_table ("
                     "'$0'::timestamptz,"
                     "null::timestamptz, null::text, 1000 , null::text[] )",
                     absl::Now() + absl::Minutes(20))));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_THAT(
      stmt->resolved_statement()->Accept(&validator),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is too far in the future.")));
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateStartTimestampTooOldBeforeRetentionNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt, AnalyzePGQuery(absl::Substitute(
                     "SELECT * FROM "
                     "spanner.read_json_change_stream_test_table ("
                     "'$0'::timestamptz,"
                     "null::timestamptz, null::text, 1000 , null::text[] )",
                     absl::Now() - absl::Hours(48))));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_THAT(
      stmt->resolved_statement()->Accept(&validator),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is too far in the past.")));
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateStartTimestampGreaterThanEndTimestampNonValid) {
  absl::Time start = absl::Now();
  absl::Time end = start - absl::Seconds(1);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt, AnalyzePGQuery(absl::Substitute(
                     "SELECT * FROM "
                     "spanner.read_json_change_stream_test_table ("
                     "'$0'::timestamptz,"
                     "'$1'::timestamptz, null::text, 1000 , null::text[] )",
                     start, end)));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_THAT(stmt->resolved_statement()->Accept(&validator),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(
                           "start_timestamp must be <= end_timestamp.")));
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateNullHeartbeatMillisecondsNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt,
      AnalyzePGQuery(absl::Substitute(
          "SELECT * FROM "
          "spanner.read_json_change_stream_test_table ("
          "'$0'::timestamptz,"
          "null::timestamptz, null::text, null::bigint, null::text[] )",
          absl::Now())));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentNullHeartbeat());
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateHeartbeatMillisecondsTooSmallNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto stmt,
                       AnalyzePGQuery(absl::Substitute(
                           "SELECT * FROM "
                           "spanner.read_json_change_stream_test_table ("
                           "'$0'::timestamptz,"
                           "null::timestamptz, null::text, 99, null::text[] )",
                           absl::Now())));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentOutOfRangeHeartbeat(
                limits::kChangeStreamsMinHeartbeatMilliseconds,
                limits::kChangeStreamsMaxHeartbeatMilliseconds, 99));
}

TEST_F(PgChangeStreamQueryValidatorTest,
       ValidateHeartbeatMillisecondsTooLargeNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto stmt, AnalyzePGQuery(absl::Substitute(
                     "SELECT * FROM "
                     "spanner.read_json_change_stream_test_table ("
                     "'$0'::timestamptz,"
                     "null::timestamptz, null::text, 300001, null::text[] )",
                     absl::Now())));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentOutOfRangeHeartbeat(
                limits::kChangeStreamsMinHeartbeatMilliseconds,
                limits::kChangeStreamsMaxHeartbeatMilliseconds, 300001));
}

TEST_F(PgChangeStreamQueryValidatorTest, ValidateNonNullReadOptionsNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto stmt,
                       AnalyzePGQuery(absl::Substitute(
                           "SELECT * FROM "
                           "spanner.read_json_change_stream_test_table ("
                           "'$0'::timestamptz,"
                           "null::timestamptz, null::text, 1000 , "
                           "array['INCLUDE_START_TIME']::text[] )",
                           absl::Now())));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentNonNullReadOptions());
}

TEST_F(PgChangeStreamQueryValidatorTest, ValidateScalarArgumentsNonValid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto stmt,
                       AnalyzePGQuery(absl::Substitute(
                           "SELECT * FROM "
                           "spanner.read_json_change_stream_test_table ("
                           "'$0'::timestamptz,"
                           "null::timestamptz, null::text,abs(100) , "
                           "null::text[] )",
                           absl::Now())));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentWithArgIndex(
                "read_json_change_stream_test_table", 3));
}

class PgChangeStreamQueryValidatorIllegalTVFTest
    : public PgChangeStreamQueryValidatorTest,
      public testing::WithParamInterface<std::string> {};

TEST_P(PgChangeStreamQueryValidatorIllegalTVFTest,
       IllegalPgChangeStreamQuerySyntax) {
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto stmt, AnalyzePGQuery(GetParam()));
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::IllegalChangeStreamQueryPGSyntax(validator.tvf_name()));
}

INSTANTIATE_TEST_SUITE_P(
    ValidatePgTVFTest, PgChangeStreamQueryValidatorIllegalTVFTest,
    testing::Values(
        absl::Substitute(
            "SELECT * FROM "
            "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
            "null::timestamptz, null::text, 1000,null::text[] ) limit 1 offset "
            "1",
            absl::Now()),
        absl::Substitute(
            "SELECT count(*) FROM "
            "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
            "null::timestamptz, null::text, 1000,null::text[] )",
            absl::Now()),
        absl::Substitute(
            "SELECT * FROM "
            "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
            "null::timestamptz, null::text, 1000,null::text[] ) UNION "
            "ALL SELECT * FROM spanner.read_json_change_stream_test_table "
            "('$0'::timestamptz, null::timestamptz, null::text, "
            "1000,null::text[] )",
            absl::Now()),
        absl::Substitute(
            "SELECT * FROM "
            "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
            "null::timestamptz, null::text, 1000,null::text[] ) as streams",
            absl::Now())));

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
