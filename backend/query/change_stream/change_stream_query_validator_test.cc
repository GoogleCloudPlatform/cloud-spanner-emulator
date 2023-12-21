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

#include "backend/query/change_stream/change_stream_query_validator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "common/errors.h"
#include "common/limits.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;
class OtherTvf : public zetasql::TableValuedFunction {
 public:
  explicit OtherTvf(std::vector<std::string> function_name_path,
                    const zetasql::FunctionSignature& signature,
                    const zetasql::TVFRelation& result_schema)
      : zetasql::TableValuedFunction(function_name_path, signature),
        result_schema_(result_schema) {}
  absl::Status Resolve(
      const zetasql::AnalyzerOptions* analyzer_options,
      const std::vector<zetasql::TVFInputArgumentType>& actual_arguments,
      const zetasql::FunctionSignature& concrete_signature,
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      std::shared_ptr<zetasql::TVFSignature>* output_tvf_signature)
      const final {
    output_tvf_signature->reset(
        new zetasql::TVFSignature(actual_arguments, result_schema_));
    return absl::OkStatus();
  }

 private:
  const zetasql::TVFRelation result_schema_;
};
}  // namespace

class ChangeStreamQueryValidatorTest : public testing::Test {
 public:
  friend class Catalog;
  ChangeStreamQueryValidatorTest()
      : analyzer_options_(MakeGoogleSqlAnalyzerOptions()),
        fn_catalog_(&type_factory_),
        schema_(
            test::CreateSchemaWithOneTableAndOneChangeStream(&type_factory_)),
        catalog_(std::make_unique<Catalog>(
            schema_.get(), &fn_catalog_, &type_factory_, analyzer_options_)) {}

 protected:
  std::unique_ptr<const zetasql::AnalyzerOutput> AnalyzeQuery(
      const std::string& sql) {
    std::unique_ptr<const zetasql::AnalyzerOutput> output;
    ZETASQL_EXPECT_OK(zetasql::AnalyzeStatement(
        sql, analyzer_options_, catalog_.get(), &type_factory_, &output));
    return output;
  }

  zetasql::TypeFactory type_factory_;

  const zetasql::AnalyzerOptions analyzer_options_;

  const FunctionCatalog fn_catalog_;

  std::unique_ptr<const Schema> schema_;

  std::unique_ptr<Catalog> catalog_;

  absl::flat_hash_map<std::string, zetasql::Value> params_;
};

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateNoneChangeStreamTvfWithSamePrefixIsFiltered) {
  std::vector<zetasql::TVFSchemaColumn> output_columns;
  output_columns.emplace_back("output", type_factory_.get_string());
  std::vector<zetasql::FunctionArgumentType> args;
  zetasql::TVFRelation result_schema(output_columns);
  const auto result_type = zetasql::FunctionArgumentType::RelationWithSchema(
      result_schema, /*extra_relation_input_columns_allowed=*/false);
  const auto signature = zetasql::FunctionSignature(result_type, args,
                                                      /*context_ptr=*/nullptr);

  catalog_->tvfs_["READ_foo"] = std::make_unique<OtherTvf>(
      absl::StrSplit("READ_foo", '.'), signature, result_schema);
  auto stmt = AnalyzeQuery(absl::Substitute("SELECT * FROM READ_foo()"));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  EXPECT_THAT(validator.IsChangeStreamQuery(stmt->resolved_statement()),
              IsOkAndHolds(false));
}

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateNoneChangeStreamTvfWithDifferentPrefixIsFiltered) {
  std::vector<zetasql::TVFSchemaColumn> output_columns;
  output_columns.emplace_back("output", type_factory_.get_string());
  std::vector<zetasql::FunctionArgumentType> args;
  zetasql::TVFRelation result_schema(output_columns);
  const auto result_type = zetasql::FunctionArgumentType::RelationWithSchema(
      result_schema, /*extra_relation_input_columns_allowed=*/false);
  const auto signature = zetasql::FunctionSignature(result_type, args,
                                                      /*context_ptr=*/nullptr);

  catalog_->tvfs_["some_tvf"] = std::make_unique<OtherTvf>(
      absl::StrSplit("some_tvf", '.'), signature, result_schema);
  auto stmt = AnalyzeQuery(absl::Substitute("SELECT * FROM some_tvf()"));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  EXPECT_THAT(validator.IsChangeStreamQuery(stmt->resolved_statement()),
              IsOkAndHolds(false));
}

TEST_F(ChangeStreamQueryValidatorTest, ValidateArgumentLiteralsValid) {
  auto stmt = AnalyzeQuery(
      absl::Substitute("SELECT ChangeRecord FROM "
                       "READ_change_stream_test_table ('$0', "
                       "'$1', 'test_token', 1000 )",
                       absl::Now(), absl::Now() + absl::Minutes(1)));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ZETASQL_ASSERT_OK(stmt->resolved_statement()->Accept(&validator));
}

TEST_F(ChangeStreamQueryValidatorTest, ValidateArgumentParametersValid) {
  auto stmt = AnalyzeQuery(
      "SELECT * FROM "
      "READ_change_stream_test_table (@startTimestamp, "
      "@endTimestamp, @partitionToken, @heartbeatMillisecond )");
  params_.insert({"heartbeatMillisecond", zetasql::Value::Int64(1000)});
  params_.insert({"partitionToken", zetasql::Value::String("test_token")});
  params_.insert({"endTimestamp",
                  zetasql::Value::Timestamp(absl::Now() + absl::Minutes(1))});
  params_.insert({"startTimestamp", zetasql::Value::Timestamp(absl::Now())});
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ZETASQL_ASSERT_OK(stmt->resolved_statement()->Accept(&validator));
}

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateMetadataCreatedForValidChangeStreamQuery) {
  absl::Time start_time = absl::Now();
  absl::Time end_time = start_time + absl::Minutes(1);
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "'$1', 'test_token', 1000 )",
                                    start_time, end_time));
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
  EXPECT_EQ(validator.change_stream_metadata().end_timestamp.value(), end_time);
  EXPECT_EQ(validator.change_stream_metadata().start_timestamp, start_time);
  EXPECT_EQ(validator.change_stream_metadata().tvf_name,
            "READ_change_stream_test_table");
  EXPECT_EQ(validator.change_stream_metadata().partition_table,
            "_change_stream_partition_change_stream_test_table");
  EXPECT_EQ(validator.change_stream_metadata().data_table,
            "_change_stream_data_change_stream_test_table");
  ASSERT_FALSE(validator.change_stream_metadata().is_pg);
  ASSERT_TRUE(validator.change_stream_metadata().is_change_stream_query);
}

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateMetadataCreatedForValidRegularQuery) {
  auto stmt = AnalyzeQuery("SELECT * FROM test_table");
  ChangeStreamQueryValidator validator{schema_.get(), absl::Now(),
                                       std::move(params_)};
  ASSERT_FALSE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  ASSERT_FALSE(validator.change_stream_metadata().is_change_stream_query);
}

TEST_F(ChangeStreamQueryValidatorTest, ValidateEmptyStartTimestampNonValid) {
  auto stmt = AnalyzeQuery(
      "SELECT * FROM "
      "READ_change_stream_test_table (NULL, "
      "NULL, NULL, 1000 )");
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

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateStartTimestampTooFarInFutureNonValid) {
  absl::Time query_start_time = absl::Now();
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "NULL, 'test_token', 1000 )",
                                    query_start_time + absl::Minutes(11)));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      query_start_time,
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

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateStartTimestampTooOldBeforeRetentionNonValid) {
  absl::Time query_start_time = absl::Now();
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "NULL, 'test_token', 1000 )",
                                    query_start_time - absl::Minutes(1)));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      query_start_time,
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

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateStartTimestampGreaterThanEndTimestampNonValid) {
  absl::Time start = absl::Now();
  absl::Time end = start - absl::Seconds(1);
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "'$1', 'test_token', 1000 )",
                                    start, end));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(
      stmt->resolved_statement()->Accept(&validator),
      error::
          InvalidChangeStreamTvfArgumentStartTimestampGreaterThanEndTimestamp(
              absl::FormatTime(start), absl::FormatTime(end)));
}

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateNullHeartbeatMillisecondsNonValid) {
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "NULL, 'test_token', NULL )",
                                    absl::Now()));
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

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateHeartbeatMillisecondsTooSmallNonValid) {
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "NULL, 'test_token', 99 )",
                                    absl::Now()));
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

TEST_F(ChangeStreamQueryValidatorTest,
       ValidateHeartbeatMillisecondsTooLargeNonValid) {
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "NULL, 'test_token', 300001 )",
                                    absl::Now()));
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

TEST_F(ChangeStreamQueryValidatorTest, ValidateNonNullReadOptionsNonValid) {
  auto stmt = AnalyzeQuery(
      absl::Substitute("SELECT * FROM "
                       "READ_change_stream_test_table ('$0', "
                       "NULL, 'test_token', 1000,['INCLUDE_START_TIME'] )",
                       absl::Now()));
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

TEST_F(ChangeStreamQueryValidatorTest, ValidateScalarArgumentNonValid) {
  auto stmt =
      AnalyzeQuery(absl::Substitute("SELECT * FROM "
                                    "READ_change_stream_test_table ('$0', "
                                    "NULL, (SELECT 'token'), 1000 )",
                                    absl::Now()));
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::InvalidChangeStreamTvfArgumentWithArgIndex(
                "READ_change_stream_test_table", 2));
}

class ChangeStreamQueryValidatorIllegalTVFTest
    : public ChangeStreamQueryValidatorTest,
      public testing::WithParamInterface<std::string> {};

TEST_P(ChangeStreamQueryValidatorIllegalTVFTest,
       IllegalChangeStreamQuerySyntax) {
  ChangeStreamQueryValidator validator{
      schema_.get(),
      absl::Now(),
      params_,
  };
  auto stmt = AnalyzeQuery(GetParam());
  ASSERT_TRUE(
      validator.IsChangeStreamQuery(stmt->resolved_statement()).value());
  EXPECT_EQ(stmt->resolved_statement()->Accept(&validator),
            error::IllegalChangeStreamQuerySyntax(validator.tvf_name()));
}

INSTANTIATE_TEST_SUITE_P(
    ValidateTVFTest, ChangeStreamQueryValidatorIllegalTVFTest,
    testing::Values(
        absl::Substitute("SELECT "
                         "ChangeRecord[ORDINAL(0)].child_partitions_record["
                         "ORDINAL(0)].child_partitions[ORDINAL(0)].token FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 )",
                         absl::Now()),
        absl::Substitute("SELECT ChangeRecord FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 ) LIMIT 1 OFFSET 1",
                         absl::Now()),
        absl::Substitute("SELECT ChangeRecord FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 ) WHERE "
                         "ChangeRecord[OFFSET(0)].child_partitions_record["
                         "OFFSET(0)].child_partitions[OFFSET(0)].token = 'abc'",
                         absl::Now()),
        absl::Substitute("SELECT "
                         "MAX(ChangeRecord[ORDINAL(0)].child_partitions_record["
                         "ORDINAL(0)].child_partitions[ORDINAL(0)].token) FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 )",
                         absl::Now()),
        absl::Substitute(
            "SELECT "
            "ChangeRecord[ORDINAL(0)].child_partitions_record["
            "ORDINAL(0)].child_partitions[ORDINAL(0)].token FROM "
            "READ_change_stream_test_table ('$0', "
            "NULL, NULL, 1000 ) JOIN test_table ON "
            "ChangeRecord[ORDINAL(0)].child_partitions_record[ORDINAL(0)]."
            "child_partitions[ORDINAL(0)].token = test_table.string_col",
            absl::Now()),
        absl::Substitute(
            "WITH stream_table AS (SELECT "
            "ChangeRecord FROM "
            "READ_change_stream_test_table ('$0', "
            "NULL, NULL, 1000 )) SELECT ChangeRecord FROM stream_table",
            absl::Now()),
        absl::Substitute("SELECT ChangeRecord FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 ) ORDER BY "
                         "ChangeRecord[ORDINAL(0)].child_partitions_record["
                         "ORDINAL(0)].child_partitions[ORDINAL(0)].token",
                         absl::Now()),
        absl::Substitute("SELECT "
                         "ChangeRecord[ORDINAL(0)].child_partitions_record["
                         "ORDINAL(0)].child_partitions[ORDINAL(0)].token FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 ) GROUP BY "
                         "ChangeRecord[ORDINAL(0)].child_partitions_record["
                         "ORDINAL(0)].child_partitions[ORDINAL(0)].token",
                         absl::Now()),
        absl::Substitute("SELECT ChangeRecord FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 ) AS stream1",
                         absl::Now()),
        absl::Substitute("SELECT ChangeRecord AS stream1 FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 )",
                         absl::Now()),
        absl::Substitute("SELECT 'ChangeRecord' FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 )",
                         absl::Now()),
        absl::Substitute("SELECT * FROM READ_change_stream_test_table ('$0',"
                         "NULL, NULL, 1000 ) UNION ALL SELECT "
                         "* FROM "
                         "READ_change_stream_test_table ('$1', "
                         "NULL, NULL, 1000 )",
                         absl::Now(), absl::Now() + absl::Minutes(1)),
        absl::Substitute("SELECT COUNT(ChangeRecord) FROM "
                         "READ_change_stream_test_table ('$0', "
                         "NULL, NULL, 1000 )",
                         absl::Now())));

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
