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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/bytes.h"
#include "google/cloud/spanner/commit_result.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/keys.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/numeric.h"
#include "google/cloud/spanner/timestamp.h"
#include "google/cloud/status_or.h"
#include "common/clock.h"
#include "frontend/converters/pg_change_streams.h"
#include "tests/common/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "tests/conformance/common/environment.h"
#include "grpcpp/client_context.h"
#include "grpcpp/support/sync_stream.h"
#include "nlohmann/json.hpp"
#include "google/protobuf/json/json.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using JsonB = ::google::cloud::spanner::JsonB;
using PgNumeric = ::google::cloud::spanner::PgNumeric;
using InsertMutationBuilder = ::google::cloud::spanner::InsertMutationBuilder;
using UpdateMutationBuilder = ::google::cloud::spanner::UpdateMutationBuilder;
using DeleteMutationBuilder = ::google::cloud::spanner::DeleteMutationBuilder;
using ReplaceMutationBuilder = ::google::cloud::spanner::ReplaceMutationBuilder;
using Json = ::nlohmann::json;

MATCHER_P(JsonContentEquals, expected_json, "") {
  std::string actual_json;
  ABSL_CHECK_OK(google::protobuf::json::MessageToJsonString(arg, &actual_json));
  // Uses Json::parse to compare to json strings rather than directly comparing
  // two strings. Prevents inconsistencies caused by whitespaces/formats
  // differences.
  return Json::parse(actual_json) == Json::parse(expected_json);
}

// Gets the commit timestamp from the commit result as absl::Time or die. First
// converts a cloud::StatusOr to absl::StatusOr, verifies the status is ok and
// returns the absl::Time value.
absl::Time GetCommitTimestampOrDie(cloud::spanner::CommitResult commit_result) {
  google::cloud::StatusOr<absl::Time> commit_timestamp =
      commit_result.commit_timestamp.get<absl::Time>();
  ABSL_CHECK_OK(ToUtilStatusOr(commit_timestamp));
  return commit_timestamp.value();
}

class PGChangeStreamTest : public DatabaseTest {
 protected:
  PGChangeStreamTest()
      : feature_flags_({.enable_postgresql_interface = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
    ABSL_CHECK_OK(PopulateTestData());
  }

  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE scalar_types_all_keys_table (
          int_val bigint NOT NULL,
          bool_val bool NOT NULL,
          bytes_val bytea NOT NULL,
          date_val date NOT NULL,
          float_val float8 NOT NULL,
          string_val varchar NOT NULL,
          timestamp_val timestamptz NOT NULL,
          PRIMARY KEY (int_val,bool_val,bytes_val,date_val,float_val,string_val,timestamp_val)
          );
        )",
        R"(
          CREATE TABLE scalar_types_table (
          int_val bigint NOT NULL PRIMARY KEY,
          bool_val bool,
          bytes_val bytea,
          date_val date,
          float_val float8,
          string_val varchar,
          timestamp_val timestamptz,
          jsonb_val jsonb,
          jsonb_arr jsonb[],
          numeric_val numeric,
          numeric_arr numeric[]
          );
        )",
        R"(
          CREATE CHANGE STREAM StreamAll FOR ALL WITH (
          value_capture_type = 'NEW_VALUES' )
        )",
    }));
    ZETASQL_ASSIGN_OR_RETURN(test_session_uri_, CreateTestSession());
    return absl::OkStatus();
  }

  std::string test_session_uri_;
  absl::Time initial_data_population_ts_;
  // Creates a new session for tests using raw grpc client.
  absl::StatusOr<std::string> CreateTestSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    spanner_api::Session response;
    request.set_database(database()->FullName());
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response.name();
  }

  // Reads from the rpc stream and adds each received partial result set to the
  // output lists. Closes the stream reader after the query terminates and
  // returns the partial result sets list only.
  absl::Status ReadFromClientReader(
      std::unique_ptr<grpc::ClientReader<spanner_api::PartialResultSet>> reader,
      std::vector<spanner_api::PartialResultSet>& response) {
    response.clear();
    spanner_api::PartialResultSet result;
    while (reader->Read(&result)) {
      response.push_back(result);
    }
    return reader->Finish();
  }

  // Builds the change stream tvf query in postgres dialect with the given
  // values for start_time, end_time, partition_token and
  // heartbeat_milliseconds. Substitutes any nullable parameters with `NULL` if
  // the given parameter value is std::nullopt.
  std::string BuildChangeStreamQuery(
      std::optional<absl::Time> start = std::nullopt,
      std::optional<absl::Time> end = std::nullopt,
      std::optional<std::string> token = std::nullopt,
      int heartbeat_milliseconds = 10000) {
    return absl::Substitute(
        "SELECT * FROM "
        "spanner.read_json_StreamAll ("
        "$0::timestamptz,"
        "$1::timestamptz, $2::text, $3 , NULL::text[] )",
        start.has_value() ? absl::StrCat("'", start.value(), "'") : "NULL",
        end.has_value() ? absl::StrCat("'", end.value(), "'") : "NULL",
        token.has_value() ? absl::StrCat("'", token.value(), "'") : "NULL",
        heartbeat_milliseconds);
  }

  // Executes the given change stream query with ExecuteStreamingSql API,
  // populates the results as a partial result set list and converts the list
  // into a list of test::ChangeStreamRecords for later content verification in
  // testing environment.
  absl::StatusOr<test::ChangeStreamRecords> ExecuteChangeStreamQuery(
      absl::string_view sql) {
    // Build the request that will be executed.
    spanner_api::ExecuteSqlRequest request;
    request.mutable_transaction()
        ->mutable_single_use()
        ->mutable_read_only()
        ->set_strong(true);
    *request.mutable_sql() = sql;
    request.set_session(test_session_uri_);

    // Execute the tvf query with ExecuteStreamingSql API.
    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader = raw_client()->ExecuteStreamingSql(&context, request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), response));

    ZETASQL_ASSIGN_OR_RETURN(auto result_set, backend::test::MergePartialResultSets(
                                          response, /*columns_per_row=*/1));
    ZETASQL_ASSIGN_OR_RETURN(ChangeStreamRecords change_records,
                     GetChangeStreamRecordsFromResultSet(result_set));
    return change_records;
  }

  // Executes a change stream initial query with start_time equals to given
  // time. Parses all the child partition tokens from the query result and
  // constructs a list of active partition tokens at the provided timestamp for
  // executing partition queries later.
  absl::StatusOr<std::vector<std::string>> GetActiveTokensFromInitialQuery(
      absl::Time time) {
    std::vector<std::string> active_tokens;
    ZETASQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                     ExecuteChangeStreamQuery(BuildChangeStreamQuery(time)));
    for (const auto& child_partition_record :
         change_records.child_partition_records) {
      for (const auto& child_partition :
           child_partition_record.child_partitions.values()) {
        active_tokens.push_back(child_partition.struct_value()
                                    .fields()
                                    .at(frontend::kToken)
                                    .string_value());
      }
    }
    return active_tokens;
  }

  // Execute a change stream partition query with start_time equals to given
  // time. First retrieve the list of active partition tokens at the given start
  // time, then execute a series of partition query with partition_token equals
  // to each of the active token list. Store all the received data change
  // records from these partition queries and return all data change records as
  // a list.
  absl::StatusOr<std::vector<DataChangeRecord>> GetDataRecordsFromStartToNow(
      absl::Time start) {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::string> active_tokens,
                     GetActiveTokensFromInitialQuery(start));
    std::vector<DataChangeRecord> merged_data_change_records;
    for (const auto& partition_token : active_tokens) {
      ZETASQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(BuildChangeStreamQuery(
                           start, Clock().Now(), partition_token)));
      merged_data_change_records.insert(
          merged_data_change_records.end(),
          change_records.data_change_records.begin(),
          change_records.data_change_records.end());
    }
    return merged_data_change_records;
  }

  absl::Status PopulateTestData() {
    Array<JsonB> jsonb_arr{JsonB(R"("1")"), JsonB(R"("2")")};
    Array<Numeric> numeric_arr{cloud::spanner::MakeNumeric("123.45").value(),
                               cloud::spanner::MakeNumeric("678.9").value()};
    auto mutation_builder = InsertMutationBuilder(
        "scalar_types_table",
        {"int_val", "bytes_val", "date_val", "float_val", "timestamp_val",
         "jsonb_val", "jsonb_arr", "numeric_val", "numeric_arr"});
    mutation_builder.AddRow(
        ValueRow{1,
                 cloud::spanner::Bytes(
                     cloud::spanner_internal::BytesFromBase64("blue").value()),
                 "2014-09-27", (float)1.1,
                 cloud::spanner::MakeTimestamp(absl::UnixEpoch()).value(),
                 JsonB(R"("3")"), jsonb_arr,
                 cloud::spanner::MakeNumeric("111.1").value(), numeric_arr});
    ZETASQL_ASSIGN_OR_RETURN(auto commit_result, Commit({mutation_builder.Build()}));
    initial_data_population_ts_ = GetCommitTimestampOrDie(commit_result);
    return absl::OkStatus();
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGChangeStreamTest, SingleInsertVerifyDataChangeRecordContent) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(initial_data_population_ts_));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(
      data_change_records[0].commit_timestamp.string_value(),
      test::EncodeTimestampString(initial_data_population_ts_, /*is_pg=*/true));
  EXPECT_EQ(data_change_records[0].record_sequence.string_value(), "00000000");
  ASSERT_TRUE(data_change_records[0]
                  .is_last_record_in_transaction_in_partition.bool_value());
  EXPECT_EQ(data_change_records[0].table_name.string_value(),
            "scalar_types_table");
  const std::string expected_col_types =
      R"json([
      {"is_primary_key":true,"name":"int_val","ordinal_position":1,"type":{"code":"INT64"}},
      {"is_primary_key":false,"name":"bool_val","ordinal_position":2,"type":{"code":"BOOL"}},
      {"is_primary_key":false,"name":"bytes_val","ordinal_position":3,"type":{"code":"BYTES"}},
      {"is_primary_key":false,"name":"date_val","ordinal_position":4,"type":{"code":"DATE"}},
      {"is_primary_key":false,"name":"float_val","ordinal_position":5,"type":{"code":"FLOAT64"}},
      {"is_primary_key":false,"name":"string_val","ordinal_position":6,"type":{"code":"STRING"}},
      {"is_primary_key":false,"name":"timestamp_val","ordinal_position":7,"type":{"code":"TIMESTAMP"}},
      {"is_primary_key":false,"name":"jsonb_val","ordinal_position":8,"type":{"code":"JSON","type_annotation":"PG_JSONB"}},
      {"is_primary_key":false,"name":"jsonb_arr","ordinal_position":9,"type":{"array_element_type":{"code":"JSON","type_annotation":"PG_JSONB"},"code":"ARRAY"}},
      {"is_primary_key":false,"name":"numeric_val","ordinal_position":10,"type":{"code":"NUMERIC","type_annotation":"PG_NUMERIC"}},
      {"is_primary_key":false,"name":"numeric_arr","ordinal_position":11,"type":{"array_element_type":{"code":"NUMERIC","type_annotation":"PG_NUMERIC"},"code":"ARRAY"}}
      ])json";
  EXPECT_THAT(data_change_records[0].column_types,
              JsonContentEquals(expected_col_types));
  const std::string expected_mods =
      R"json([
      {"keys":{"int_val":"1"},
      "old_values":{},
      "new_values":{
      "bytes_val":"blue",
      "bool_val":null,
      "date_val":"2014-09-27",
      "float_val":1.1000000238418579,
      "jsonb_arr":["\"1\"","\"2\""],
      "jsonb_val":"\"3\"",
      "numeric_arr":["123.45","678.9"],
      "numeric_val":"111.1",
      "string_val":null,
      "timestamp_val":"1970-01-01T00:00:00Z"
      }}
      ])json";
  EXPECT_THAT(data_change_records[0].mods, JsonContentEquals(expected_mods));
  EXPECT_EQ(data_change_records[0].mod_type.string_value(), "INSERT");
  EXPECT_EQ(data_change_records[0].value_capture_type.string_value(),
            "NEW_VALUES");
  EXPECT_EQ(
      data_change_records[0].number_of_records_in_transaction.number_value(),
      1);
  EXPECT_EQ(
      data_change_records[0].number_of_partitions_in_transaction.number_value(),
      1);
  EXPECT_EQ(data_change_records[0].transaction_tag.string_value(), "");
  ASSERT_FALSE(data_change_records[0].is_system_transaction.bool_value());
}

TEST_F(PGChangeStreamTest, SingleUpdateVerifyDataChangeRecordContent) {
  Array<Numeric> numeric_arr{cloud::spanner::MakeNumeric("88").value(),
                             cloud::spanner::MakeNumeric("77").value()};
  auto mutation_builder = UpdateMutationBuilder(
      "scalar_types_table",
      {"int_val", "bool_val", "bytes_val", "date_val", "float_val",
       "string_val", "timestamp_val", "jsonb_val", "jsonb_arr", "numeric_val",
       "numeric_arr"});
  mutation_builder.AddRow(ValueRow{
      1, true,
      cloud::spanner::Bytes(
          cloud::spanner_internal::BytesFromBase64("Zm9vYmFy").value()),
      "2015-09-27", (float)2.2, "hello", Null<Timestamp>(),
      JsonB(R"({"a": [2], "b": [1]})"), Null<Array<JsonB>>(),
      cloud::spanner::MakeNumeric("999.9").value(), numeric_arr});
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result, Commit({mutation_builder.Build()}));
  const absl::Time commit_ts = GetCommitTimestampOrDie(commit_result);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<DataChangeRecord> data_change_records,
                       GetDataRecordsFromStartToNow(commit_ts));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].commit_timestamp.string_value(),
            test::EncodeTimestampString(commit_ts, /*is_pg=*/true));
  EXPECT_EQ(data_change_records[0].record_sequence.string_value(), "00000000");
  ASSERT_TRUE(data_change_records[0]
                  .is_last_record_in_transaction_in_partition.bool_value());
  EXPECT_EQ(data_change_records[0].table_name.string_value(),
            "scalar_types_table");
  const std::string expected_col_types =
      R"json([
      {"is_primary_key":true,"name":"int_val","ordinal_position":1,"type":{"code":"INT64"}},
      {"is_primary_key":false,"name":"bool_val","ordinal_position":2,"type":{"code":"BOOL"}},
      {"is_primary_key":false,"name":"bytes_val","ordinal_position":3,"type":{"code":"BYTES"}},
      {"is_primary_key":false,"name":"date_val","ordinal_position":4,"type":{"code":"DATE"}},
      {"is_primary_key":false,"name":"float_val","ordinal_position":5,"type":{"code":"FLOAT64"}},
      {"is_primary_key":false,"name":"string_val","ordinal_position":6,"type":{"code":"STRING"}},
      {"is_primary_key":false,"name":"timestamp_val","ordinal_position":7,"type":{"code":"TIMESTAMP"}},
      {"is_primary_key":false,"name":"jsonb_val","ordinal_position":8,"type":{"code":"JSON","type_annotation":"PG_JSONB"}},
      {"is_primary_key":false,"name":"jsonb_arr","ordinal_position":9,"type":{"array_element_type":{"code":"JSON","type_annotation":"PG_JSONB"},"code":"ARRAY"}},
      {"is_primary_key":false,"name":"numeric_val","ordinal_position":10,"type":{"code":"NUMERIC","type_annotation":"PG_NUMERIC"}},
      {"is_primary_key":false,"name":"numeric_arr","ordinal_position":11,"type":{"array_element_type":{"code":"NUMERIC","type_annotation":"PG_NUMERIC"},"code":"ARRAY"}}
      ])json";
  EXPECT_THAT(data_change_records[0].column_types,
              JsonContentEquals(expected_col_types));
  const std::string expected_mods =
      R"json([
      {"keys":{"int_val":"1"},
      "old_values":{},
      "new_values":{
      "bytes_val":"Zm9vYmFy",
      "bool_val":true,
      "date_val":"2015-09-27",
      "float_val":2.2000000476837158,
      "jsonb_arr":null,
      "jsonb_val":"{\"a\": [2], \"b\": [1]}",
      "numeric_arr":["88","77"],
      "numeric_val":"999.9",
      "string_val":"hello",
      "timestamp_val":null
      }}
      ])json";
  EXPECT_THAT(data_change_records[0].mods, JsonContentEquals(expected_mods));
  EXPECT_EQ(data_change_records[0].mod_type.string_value(), "UPDATE");
  EXPECT_EQ(data_change_records[0].value_capture_type.string_value(),
            "NEW_VALUES");
  EXPECT_EQ(
      data_change_records[0].number_of_records_in_transaction.number_value(),
      1);
  EXPECT_EQ(
      data_change_records[0].number_of_partitions_in_transaction.number_value(),
      1);
  EXPECT_EQ(data_change_records[0].transaction_tag.string_value(), "");
  ASSERT_FALSE(data_change_records[0].is_system_transaction.bool_value());
}

TEST_F(PGChangeStreamTest, SingleDeleteVerifyDataChangeRecordContent) {
  auto mutation_builder = DeleteMutationBuilder(
      "scalar_types_table", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result, Commit({mutation_builder.Build()}));
  const absl::Time commit_ts = GetCommitTimestampOrDie(commit_result);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecordsFromStartToNow(commit_ts));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].commit_timestamp.string_value(),
            test::EncodeTimestampString(commit_ts, /*is_pg=*/true));
  EXPECT_EQ(data_change_records[0].record_sequence.string_value(), "00000000");
  ASSERT_TRUE(data_change_records[0]
                  .is_last_record_in_transaction_in_partition.bool_value());
  EXPECT_EQ(data_change_records[0].table_name.string_value(),
            "scalar_types_table");
  const std::string expected_col_types =
      R"json([
      {"is_primary_key":true,"name":"int_val","ordinal_position":1,"type":{"code":"INT64"}},
      {"is_primary_key":false,"name":"bool_val","ordinal_position":2,"type":{"code":"BOOL"}},
      {"is_primary_key":false,"name":"bytes_val","ordinal_position":3,"type":{"code":"BYTES"}},
      {"is_primary_key":false,"name":"date_val","ordinal_position":4,"type":{"code":"DATE"}},
      {"is_primary_key":false,"name":"float_val","ordinal_position":5,"type":{"code":"FLOAT64"}},
      {"is_primary_key":false,"name":"string_val","ordinal_position":6,"type":{"code":"STRING"}},
      {"is_primary_key":false,"name":"timestamp_val","ordinal_position":7,"type":{"code":"TIMESTAMP"}},
      {"is_primary_key":false,"name":"jsonb_val","ordinal_position":8,"type":{"code":"JSON","type_annotation":"PG_JSONB"}},
      {"is_primary_key":false,"name":"jsonb_arr","ordinal_position":9,"type":{"array_element_type":{"code":"JSON","type_annotation":"PG_JSONB"},"code":"ARRAY"}},
      {"is_primary_key":false,"name":"numeric_val","ordinal_position":10,"type":{"code":"NUMERIC","type_annotation":"PG_NUMERIC"}},
      {"is_primary_key":false,"name":"numeric_arr","ordinal_position":11,"type":{"array_element_type":{"code":"NUMERIC","type_annotation":"PG_NUMERIC"},"code":"ARRAY"}}
      ])json";
  EXPECT_THAT(data_change_records[0].column_types,
              JsonContentEquals(expected_col_types));
  const std::string expected_mods =
      R"json([
      {"new_values":{},"keys":{"int_val":"1"},"old_values":{}}
      ])json";
  EXPECT_THAT(data_change_records[0].mods, JsonContentEquals(expected_mods));
  EXPECT_EQ(data_change_records[0].mod_type.string_value(), "DELETE");
  EXPECT_EQ(data_change_records[0].value_capture_type.string_value(),
            "NEW_VALUES");
  EXPECT_EQ(
      data_change_records[0].number_of_records_in_transaction.number_value(),
      1);
  EXPECT_EQ(
      data_change_records[0].number_of_partitions_in_transaction.number_value(),
      1);
  EXPECT_EQ(data_change_records[0].transaction_tag.string_value(), "");
  EXPECT_FALSE(data_change_records[0].is_system_transaction.bool_value());
}

TEST_F(PGChangeStreamTest, DiffDataTypesInKey) {
  auto mutation_builder =
      InsertMutationBuilder("scalar_types_all_keys_table",
                            {"int_val", "bool_val", "bytes_val", "date_val",
                             "float_val", "string_val", "timestamp_val"});
  mutation_builder.AddRow(
      ValueRow{1, true,
               cloud::spanner::Bytes(
                   cloud::spanner_internal::BytesFromBase64("blue").value()),
               "2014-09-27", (float)1.1, "test_str",
               cloud::spanner::MakeTimestamp(absl::UnixEpoch()).value()});
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result, Commit({mutation_builder.Build()}));
  absl::Time commit_ts = GetCommitTimestampOrDie(commit_result);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecordsFromStartToNow(commit_ts));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].commit_timestamp.string_value(),
            test::EncodeTimestampString(commit_ts, /*is_pg=*/true));
  EXPECT_EQ(data_change_records[0].record_sequence.string_value(), "00000000");
  ASSERT_TRUE(data_change_records[0]
                  .is_last_record_in_transaction_in_partition.bool_value());
  EXPECT_EQ(data_change_records[0].table_name.string_value(),
            "scalar_types_all_keys_table");
  const std::string expected_col_types =
      R"json([
      {"is_primary_key":true,"name":"int_val","ordinal_position":1,"type":{"code":"INT64"}},
      {"is_primary_key":true,"name":"bool_val","ordinal_position":2,"type":{"code":"BOOL"}},
      {"is_primary_key":true,"name":"bytes_val","ordinal_position":3,"type":{"code":"BYTES"}},
      {"is_primary_key":true,"name":"date_val","ordinal_position":4,"type":{"code":"DATE"}},
      {"is_primary_key":true,"name":"float_val","ordinal_position":5,"type":{"code":"FLOAT64"}},
      {"is_primary_key":true,"name":"string_val","ordinal_position":6,"type":{"code":"STRING"}},
      {"is_primary_key":true,"name":"timestamp_val","ordinal_position":7,"type":{"code":"TIMESTAMP"}}
      ])json";
  EXPECT_THAT(data_change_records[0].column_types,
              JsonContentEquals(expected_col_types));

  const std::string expected_mods =
      R"json([
      {
      "new_values":{},
      "keys":{
      "int_val":"1",
      "bool_val":true,
      "bytes_val":"blue",
      "date_val":"2014-09-27",
      "float_val":1.1000000238418579,
      "string_val":"test_str",
      "timestamp_val":"1970-01-01T00:00:00Z"
      },
      "old_values":{}}
      ])json";

  EXPECT_THAT(data_change_records[0].mods, JsonContentEquals(expected_mods));
  EXPECT_EQ(data_change_records[0].mod_type.string_value(), "INSERT");
  EXPECT_EQ(data_change_records[0].value_capture_type.string_value(),
            "NEW_VALUES");
  EXPECT_EQ(
      data_change_records[0].number_of_records_in_transaction.number_value(),
      1);
  EXPECT_EQ(
      data_change_records[0].number_of_partitions_in_transaction.number_value(),
      1);
  EXPECT_EQ(data_change_records[0].transaction_tag.string_value(), "");
  ASSERT_FALSE(data_change_records[0].is_system_transaction.bool_value());
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
