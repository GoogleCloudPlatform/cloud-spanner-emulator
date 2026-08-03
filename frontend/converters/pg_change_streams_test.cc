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

#include "frontend/converters/pg_change_streams.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "frontend/converters/change_streams.h"
#include "tests/common/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/common/row_cursor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

using ::google::spanner::emulator::test::TestRowCursor;
using ::google::spanner::v1::PartialResultSet;
using googlesql::types::BoolType;
using googlesql::types::Int64Type;
using googlesql::types::StringArrayType;
using googlesql::types::StringType;
using googlesql::types::TimestampType;
using googlesql::values::Bool;
using googlesql::values::Int64;
using googlesql::values::String;
using googlesql::values::Timestamp;
using testing::ElementsAre;
using test::EqualsProto;
using test::proto::Partially;
using ::googlesql_base::testing::StatusIs;

class PgChangeStreamResultConverterTest : public testing::Test {
 protected:
  void SetUp() override {
    now_ = absl::Now();
    one_min_from_now_ = now_ + absl::Minutes(1);
  }
  google::spanner::v1::ResultSet ConvertPartialResultSetToResultSet(
      PartialResultSet& partial_result) {
    google::spanner::v1::ResultSet result_pb;
    for (const auto& val : partial_result.values()) {
      auto* row_pb = result_pb.add_rows();
      *row_pb->add_values() = val;
    }
    return result_pb;
  }

  absl::Time now_;
  absl::Time one_min_from_now_;
  static constexpr char kDummyChangeStreamJsonTvf[] = "read_json_dummy_cs";
  static constexpr char kDummyChangeStreamBytesTvf[] = "read_bytes_dummy_cs";
};

TEST_F(PgChangeStreamResultConverterTest,
       PopulateFixedOutputColumnTypeMetadataForFirstResponse) {
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertHeartbeatTimestampToJson(
                           now_, /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/true));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(metadata {
                 row_type {
                   fields {
                     name: "$0"
                     type { code: JSON type_annotation: PG_JSONB }
                   }
                 }
               }
               values {
                 string_value: "{\"heartbeat_record\":{\"timestamp\":\"$1\"}}"
               }
               resume_token: "$2"
          )pb",
          kDummyChangeStreamJsonTvf, now_, kChangeStreamDummyResumeToken))));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertHearbeatTimestampToChangeRecordResultSetProto) {
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      results,
      google::spanner::emulator::frontend::ConvertHeartbeatTimestampToJson(
          now_, /*tvf_name=*/kDummyChangeStreamJsonTvf));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(values {
                 string_value: "{\"heartbeat_record\":{\"timestamp\":\"$0\"}}"
               }
               resume_token: "$1"
          )pb",
          now_, kChangeStreamDummyResumeToken))));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 1);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
  EXPECT_EQ(change_recods.heartbeat_records[0].timestamp.string_value(),
            absl::FormatTime(now_));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertInitialPartitionTableRowCursorToMultipleChangeRecordsResultSet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(StringArrayType(), {String("parent1")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val2,
      googlesql::Value::MakeArray(StringArrayType(), {String("parent2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val},
       {Timestamp(now_), String("token2"), parents_array_val2}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToJson(
                           &cursor, /*initial_start_time=*/one_min_from_now_,
                           /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/false));
  const std::string expected_json1 = absl::Substitute(R"json({
    "child_partitions_record": {
      "start_timestamp": "$0",
      "record_sequence": "00000000",
      "child_partitions": [
        {
          "token": "token1",
          "parent_partition_tokens": []
        }
      ]
    }
    })json",
                                                      one_min_from_now_);
  const std::string expected_json2 = absl::Substitute(R"json({
    "child_partitions_record": {
      "start_timestamp": "$0",
      "record_sequence": "00000001",
      "child_partitions": [
        {
          "token": "token2",
          "parent_partition_tokens": []
        }
      ]
    }
    })json",
                                                      one_min_from_now_);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].values_size(), 2);
  EXPECT_EQ(JSON::parse(expected_json1),
            JSON::parse(results[0].values(0).string_value()));
  EXPECT_EQ(JSON::parse(expected_json2),
            JSON::parse(results[0].values(1).string_value()));
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 2);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMoveEventPartitionTableRowCursorToChangeRecordResultSet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(StringArrayType(), {String("move_token1")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToJson(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/false));
  const std::string expected_json = absl::Substitute(R"json({
    "child_partitions_record": {
      "start_timestamp": "$0",
      "record_sequence": "00000000",
      "child_partitions": [
        {
          "token": "token1",
          "parent_partition_tokens": ["move_token1"]
        }
      ]
    }
    })json",
                                                     now_);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].values_size(), 1);
  EXPECT_EQ(JSON::parse(expected_json),
            JSON::parse(results[0].values(0).string_value()));
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMergeEventPartitionTableRowCursorToChangeRecordResultSet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(
          StringArrayType(), {String("merge_token1"), String("merge_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToJson(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  const std::string expected_json = absl::Substitute(R"json({
    "child_partitions_record": {
      "start_timestamp": "$0",
      "record_sequence": "00000000",
      "child_partitions": [
        {
          "token": "token1",
          "parent_partition_tokens": ["merge_token1","merge_token2"]
        }
      ]
    }
    })json",
                                                     now_);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].values_size(), 1);
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  EXPECT_EQ(JSON::parse(expected_json),
            JSON::parse(results[0].values(0).string_value()));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertSplitEventPartitionTableRowCursorToChangeRecordResultSet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(StringArrayType(), {String("parent_token")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("split_token1"), parents_array_val},
       {Timestamp(now_), String("split_token2"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToJson(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  const std::string expected_json = absl::Substitute(R"json({
    "child_partitions_record": {
      "start_timestamp": "$0",
      "record_sequence": "00000000",
      "child_partitions": [
        {
          "token": "split_token1",
          "parent_partition_tokens": ["parent_token"]
        },
        {
          "token": "split_token2",
          "parent_partition_tokens": ["parent_token"]
        }
      ]
    }
    })json",
                                                     now_);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].values_size(), 1);
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  EXPECT_EQ(JSON::parse(expected_json),
            JSON::parse(results[0].values(0).string_value()));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertNewValuesDataTableRowCursorToChangeRecordResultSet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_name_arr_val,
      googlesql::Value::MakeArray(StringArrayType(),
                                  {String("IsPrimaryUser"), String("UserId")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_type_arr_val,
      googlesql::Value::MakeArray(
          StringArrayType(),
          {String("{\"code\":\"BOOL\"}"), String("{\"code\":\"STRING\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_is_primary_key_arr_val,
      googlesql::Value::MakeArray(googlesql::types::BoolArrayType(),
                                  {Bool(false), Bool(true)}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_ordinal_position_arr_val,
      googlesql::Value::MakeArray(googlesql::types::Int64ArrayType(),
                                  {Int64(1), Int64(2)}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto mods_keys,
      googlesql::Value::MakeArray(StringArrayType(),
                                  {String("{\"UserId\": \"User2\"}"),
                                   String("{\"UserId\": \"User2\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto mods_new_values,
      googlesql::Value::MakeArray(
          StringArrayType(),
          {String("{\"IsPrimaryUser\": true,\"UserId\": \"User2\"}"),
           String("{\"IsPrimaryUser\": false}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto mods_old_values,
                       googlesql::Value::MakeArray(
                           StringArrayType(), {String("{}"), String("{}")}));
  TestRowCursor cursor(
      {"partition_token", "commit_timestamp", "server_transaction_id",
       "record_sequence", "is_last_record_in_transaction_in_partition",
       "table_name", "column_types_name", "column_types_type",
       "column_type_is_primary_key", "column_types_ordinal_position",
       "mods_keys", "mods_new_values", "mods_old_values", "mod_type",
       "value_capture_type", "number_of_records_in_transaction",
       "number_of_partitions_in_transaction", "transaction_tag",
       "is_system_transaction"},
      {StringType(), TimestampType(), StringType(), StringType(), BoolType(),
       StringType(), StringType(), StringType(), BoolType(), Int64Type(),
       StringType(), StringType(), StringType(), StringType(), StringType(),
       Int64Type(), Int64Type(), StringType(), BoolType()},
      {{String("test_token"), Timestamp(now_), String("test_id"),
        String("00000001"), Bool(false), String("test_table"),
        col_types_name_arr_val, col_types_type_arr_val,
        col_types_is_primary_key_arr_val, col_types_ordinal_position_arr_val,
        mods_keys, mods_new_values, mods_old_values, String("UPDATE"),
        String("NEW_VALUES"), Int64(3), Int64(2), String("test_tag"),
        Bool(false)}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertDataTableRowCursorToJson(
                           &cursor, /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/true));
  const std::string expected_json = absl::Substitute(R"json({
    "data_change_record": {
    "commit_timestamp": "$0",
    "record_sequence": "00000001",
    "server_transaction_id": "test_id",
    "is_last_record_in_transaction_in_partition": false,
    "table_name": "test_table",
    "column_types": [
      {
        "name": "IsPrimaryUser",
        "type": {"code": "BOOL"},
        "is_primary_key": false,
        "ordinal_position": 1
      },
      {
        "name": "UserId",
        "type": {"code":"STRING"},
        "is_primary_key": true,
        "ordinal_position": 2
      }
    ],
    "mods": [
      {
        "keys": {"UserId":"User2"},
        "new_values": {"IsPrimaryUser":true,"UserId":"User2"},
        "old_values": {}
      },
      {
        "keys": {"UserId":"User2"},
        "new_values": {"IsPrimaryUser":false},
        "old_values": {}
      }
    ],
    "mod_type": "UPDATE",
    "value_capture_type": "NEW_VALUES",
    "number_of_records_in_transaction": 3,
    "number_of_partitions_in_transaction": 2,
    "transaction_tag": "test_tag",
    "is_system_transaction": false
    }
  })json",
                                                     now_);
  EXPECT_EQ(results.size(), 1);
  EXPECT_THAT(results[0].metadata(),
              EqualsProto(absl::Substitute(
                  R"pb(row_type {
                         fields {
                           name: "$0"
                           type { code: JSON type_annotation: PG_JSONB }
                         }
                       }
                  )pb",
                  kDummyChangeStreamJsonTvf)));
  EXPECT_EQ(results[0].values_size(), 1);
  EXPECT_EQ(JSON::parse(expected_json),
            JSON::parse(results[0].values(0).string_value()));
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 1);
}

TEST_F(PgChangeStreamResultConverterTest,
       PopulateBytesMetadataForFirstResponse) {
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertHeartbeatTimestampToBytes(
                           now_, /*tvf_name=*/kDummyChangeStreamBytesTvf,
                           /*expect_metadata=*/true));
  EXPECT_THAT(results, ElementsAre(Partially(EqualsProto(absl::Substitute(
                           R"pb(metadata {
                                  row_type {
                                    fields {
                                      name: "$0"
                                      type { code: BYTES }
                                    }
                                  }
                                })pb",
                           kDummyChangeStreamBytesTvf)))));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertInitialPartitionToMultipleChangeRecords_MutableKeyRange) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(StringArrayType(), {String("parent1")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val2,
      googlesql::Value::MakeArray(StringArrayType(), {String("parent2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val},
       {Timestamp(now_), String("token2"), parents_array_val2}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToBytes(
                           &cursor, /*initial_start_time=*/one_min_from_now_,
                           /*partition_token=*/"",
                           /*tvf_name=*/kDummyChangeStreamBytesTvf,
                           /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  int64_t s = absl::ToUnixSeconds(one_min_from_now_);
  int64_t ns =
      absl::ToInt64Nanoseconds(one_min_from_now_ - absl::FromUnixSeconds(s));
  ASSERT_EQ(change_records.partition_start_records.size(), 2);
  EXPECT_THAT(
      change_records.partition_start_records[0],
      EqualsProto(absl::Substitute(R"pb(
                                     start_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000000"
                                     partition_tokens: "token1"
                                   )pb",
                                   s, ns)));
  EXPECT_THAT(
      change_records.partition_start_records[1],
      EqualsProto(absl::Substitute(R"pb(
                                     start_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000001"
                                     partition_tokens: "token2"
                                   )pb",
                                   s, ns)));
  ASSERT_EQ(change_records.partition_event_records.size(), 0);
  ASSERT_EQ(change_records.partition_end_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMoveEventPartitionToChangeRecord_MutableKeyRange) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(StringArrayType(), {String("move_token1")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  EXPECT_THAT(ConvertPartitionTableRowCursorToBytes(
                  &cursor, /*initial_start_time=*/std::nullopt,
                  /*partition_token=*/"move_token1",
                  /*tvf_name=*/kDummyChangeStreamBytesTvf,
                  /*expect_metadata=*/true),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMergeEventPartitionFirstParentToChangeRecord_MutableKeyRange) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(
          StringArrayType(), {String("merge_token1"), String("merge_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToBytes(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*partition_token=*/"merge_token1",
                           /*tvf_name=*/kDummyChangeStreamBytesTvf,
                           /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));

  int64_t s = absl::ToUnixSeconds(now_);
  int64_t ns = absl::ToInt64Nanoseconds(now_ - absl::FromUnixSeconds(s));
  ASSERT_EQ(change_records.partition_start_records.size(), 1);
  EXPECT_THAT(
      change_records.partition_start_records[0],
      EqualsProto(absl::Substitute(R"pb(
                                     start_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000000"
                                     partition_tokens: "token1"
                                   )pb",
                                   s, ns)));

  ASSERT_EQ(change_records.partition_event_records.size(), 1);
  EXPECT_THAT(change_records.partition_event_records[0],
              EqualsProto(absl::Substitute(
                  R"pb(
                    commit_timestamp { seconds: $0 nanos: $1 }
                    record_sequence: "00000001"
                    partition_token: "merge_token1"
                    move_out_events { destination_partition_token: "token1" }
                  )pb",
                  s, ns)));

  ASSERT_EQ(change_records.partition_end_records.size(), 1);
  EXPECT_THAT(
      change_records.partition_end_records[0],
      EqualsProto(absl::Substitute(R"pb(
                                     end_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000002"
                                     partition_token: "merge_token1"
                                   )pb",
                                   s, ns)));

  ASSERT_EQ(change_records.data_change_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMergeEventPartitionSecondParentToChangeRecord_MutableKeyRange) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(
          StringArrayType(), {String("merge_token1"), String("merge_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToBytes(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*partition_token=*/"merge_token2",
                           /*tvf_name=*/kDummyChangeStreamBytesTvf,
                           /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));

  int64_t s = absl::ToUnixSeconds(now_);
  int64_t ns = absl::ToInt64Nanoseconds(now_ - absl::FromUnixSeconds(s));
  ASSERT_EQ(change_records.partition_start_records.size(), 0);

  ASSERT_EQ(change_records.partition_event_records.size(), 1);
  EXPECT_THAT(change_records.partition_event_records[0],
              EqualsProto(absl::Substitute(
                  R"pb(
                    commit_timestamp { seconds: $0 nanos: $1 }
                    record_sequence: "00000000"
                    partition_token: "merge_token2"
                    move_out_events { destination_partition_token: "token1" }
                  )pb",
                  s, ns)));

  ASSERT_EQ(change_records.partition_end_records.size(), 1);
  EXPECT_THAT(
      change_records.partition_end_records[0],
      EqualsProto(absl::Substitute(R"pb(
                                     end_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000001"
                                     partition_token: "merge_token2"
                                   )pb",
                                   s, ns)));

  ASSERT_EQ(change_records.data_change_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertSplitEventPartitionToChangeRecord_MutableKeyRange) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      googlesql::Value::MakeArray(StringArrayType(), {String("parent_token")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("split_token1"), parents_array_val},
       {Timestamp(now_), String("split_token2"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToBytes(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*partition_token=*/"parent_token",
                           /*tvf_name=*/kDummyChangeStreamBytesTvf,
                           /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));

  int64_t s = absl::ToUnixSeconds(now_);
  int64_t ns = absl::ToInt64Nanoseconds(now_ - absl::FromUnixSeconds(s));
  ASSERT_EQ(change_records.partition_start_records.size(), 2);
  EXPECT_THAT(
      change_records.partition_start_records[0],
      EqualsProto(absl::Substitute(R"pb(
                                     start_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000000"
                                     partition_tokens: "split_token1"
                                   )pb",
                                   s, ns)));
  EXPECT_THAT(
      change_records.partition_start_records[1],
      EqualsProto(absl::Substitute(R"pb(
                                     start_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000001"
                                     partition_tokens: "split_token2"
                                   )pb",
                                   s, ns)));

  ASSERT_EQ(change_records.partition_event_records.size(), 1);
  EXPECT_THAT(
      change_records.partition_event_records[0],
      EqualsProto(absl::Substitute(
          R"pb(
            commit_timestamp { seconds: $0 nanos: $1 }
            record_sequence: "00000002"
            partition_token: "parent_token"
            move_out_events { destination_partition_token: "split_token1" }
            move_out_events { destination_partition_token: "split_token2" }
          )pb",
          s, ns)));

  ASSERT_EQ(change_records.partition_end_records.size(), 1);
  EXPECT_THAT(
      change_records.partition_end_records[0],
      EqualsProto(absl::Substitute(R"pb(
                                     end_timestamp { seconds: $0 nanos: $1 }
                                     record_sequence: "00000003"
                                     partition_token: "parent_token"
                                   )pb",
                                   s, ns)));

  ASSERT_EQ(change_records.data_change_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
}

TEST_F(
    PgChangeStreamResultConverterTest,
    ConvertQueryStartPartitionTableRowCursorToBytes_QueryStartBeforePartition) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto parents_array_val,
                       googlesql::Value::MakeArray(
                           StringArrayType(),
                           {String("parent_token1"), String("parent_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("child_token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results, ConvertQueryStartPartitionTableRowCursorToBytes(
                                    &cursor, now_,
                                    /*tvf_name=*/kDummyChangeStreamBytesTvf,
                                    /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));

  int64_t s = absl::ToUnixSeconds(now_);
  int64_t ns = absl::ToInt64Nanoseconds(now_ - absl::FromUnixSeconds(s));
  ASSERT_EQ(change_records.partition_event_records.size(), 1);
  EXPECT_THAT(change_records.partition_event_records[0],
              EqualsProto(absl::Substitute(
                  R"pb(
                    commit_timestamp { seconds: $0 nanos: $1 }
                    record_sequence: "00000000"
                    partition_token: "child_token1"
                    move_in_events { source_partition_token: "parent_token1" }
                    move_in_events { source_partition_token: "parent_token2" }
                  )pb",
                  s, ns)));

  ASSERT_EQ(change_records.partition_start_records.size(), 0);
  ASSERT_EQ(change_records.partition_end_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(
    PgChangeStreamResultConverterTest,
    ConvertQueryStartPartitionTableRowCursorToBytes_QueryStartAfterPartition) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto parents_array_val,
                       googlesql::Value::MakeArray(
                           StringArrayType(),
                           {String("parent_token1"), String("parent_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("child_token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  absl::Time query_start_time = now_ + absl::Seconds(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results, ConvertQueryStartPartitionTableRowCursorToBytes(
                                    &cursor, query_start_time,
                                    /*tvf_name=*/kDummyChangeStreamBytesTvf,
                                    /*expect_metadata=*/true));
  EXPECT_TRUE(results.empty());
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertQueryStartPartitionTableRowCursorToBytes_QueryStartAfterStart) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto parents_array_val,
                       googlesql::Value::MakeArray(
                           StringArrayType(),
                           {String("parent_token1"), String("parent_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("child_token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  absl::Time query_start_time = now_ + absl::Seconds(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results, ConvertQueryStartPartitionTableRowCursorToBytes(
                                    &cursor, query_start_time,
                                    /*tvf_name=*/kDummyChangeStreamBytesTvf,
                                    /*expect_metadata=*/true));
  EXPECT_TRUE(results.empty());
}

TEST_F(PgChangeStreamResultConverterTest,
       HearbeatTimestampToChangeRecordProto_MutableKeyRange) {
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertHeartbeatTimestampToBytes(
                           now_, /*tvf_name=*/kDummyChangeStreamBytesTvf,
                           /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  int64_t s = absl::ToUnixSeconds(now_);
  int64_t ns = absl::ToInt64Nanoseconds(now_ - absl::FromUnixSeconds(s));
  EXPECT_THAT(change_records.mutable_key_range_heartbeat_records,
              ElementsAre(EqualsProto(
                  absl::Substitute(R"pb(
                                     timestamp { seconds: $0 nanos: $1 }
                                   )pb",
                                   s, ns))));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertNewValuesDataTableToChangeRecord_MutableKeyRange) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_name_arr_val,
      googlesql::Value::MakeArray(StringArrayType(),
                                  {String("IsPrimaryUser"), String("UserId")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_type_arr_val,
      googlesql::Value::MakeArray(
          StringArrayType(),
          {String("{\"code\":\"BOOL\"}"), String("{\"code\":\"STRING\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_is_primary_key_arr_val,
      googlesql::Value::MakeArray(googlesql::types::BoolArrayType(),
                                  {Bool(false), Bool(true)}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_ordinal_position_arr_val,
      googlesql::Value::MakeArray(googlesql::types::Int64ArrayType(),
                                  {Int64(1), Int64(2)}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto mods_keys,
      googlesql::Value::MakeArray(StringArrayType(),
                                  {String("{\"UserId\": \"User2\"}"),
                                   String("{\"UserId\": \"User2\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto mods_new_values,
      googlesql::Value::MakeArray(
          StringArrayType(),
          {String("{\"IsPrimaryUser\": true,\"UserId\": \"User2\"}"),
           String("{\"IsPrimaryUser\": false}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto mods_old_values,
                       googlesql::Value::MakeArray(
                           StringArrayType(), {String("{}"), String("{}")}));
  TestRowCursor cursor(
      {"partition_token", "commit_timestamp", "server_transaction_id",
       "record_sequence", "is_last_record_in_transaction_in_partition",
       "table_name", "column_types_name", "column_types_type",
       "column_type_is_primary_key", "column_types_ordinal_position",
       "mods_keys", "mods_new_values", "mods_old_values", "mod_type",
       "value_capture_type", "number_of_records_in_transaction",
       "number_of_partitions_in_transaction", "transaction_tag",
       "is_system_transaction"},
      {StringType(), TimestampType(), StringType(), StringType(), BoolType(),
       StringType(), StringType(), StringType(), BoolType(), Int64Type(),
       StringType(), StringType(), StringType(), StringType(), StringType(),
       Int64Type(), Int64Type(), StringType(), BoolType()},
      {{String("test_token"), Timestamp(now_), String("test_id"),
        String("00000001"), Bool(false), String("test_table"),
        col_types_name_arr_val, col_types_type_arr_val,
        col_types_is_primary_key_arr_val, col_types_ordinal_position_arr_val,
        mods_keys, mods_new_values, mods_old_values, String("UPDATE"),
        String("NEW_VALUES"), Int64(3), Int64(2), String("test_tag"),
        Bool(false)}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results, ConvertDataTableRowCursorToBytes(
                                    &cursor,
                                    /*tvf_name=*/kDummyChangeStreamBytesTvf,
                                    /*expect_metadata=*/true));
  EXPECT_FALSE(results.empty());
  EXPECT_EQ(results[0].resume_token(), kChangeStreamDummyResumeToken);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  int64_t s = absl::ToUnixSeconds(now_);
  int64_t ns = absl::ToInt64Nanoseconds(now_ - absl::FromUnixSeconds(s));
  EXPECT_THAT(change_records.mutable_key_range_data_change_records,
              ElementsAre(Partially(EqualsProto(absl::Substitute(
                  R"pb(
                    commit_timestamp { seconds: $0 nanos: $1 }
                    record_sequence: "00000001"
                    server_transaction_id: "test_id"
                    is_last_record_in_transaction_in_partition: false
                    table: "test_table"
                    mod_type: UPDATE
                    value_capture_type: NEW_VALUES
                    number_of_records_in_transaction: 3
                    number_of_partitions_in_transaction: 2
                    transaction_tag: "test_tag"
                    is_system_transaction: false
                  )pb",
                  s, ns)))));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertDataTableRowCursorToBytes_NoMetadata) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_name_arr_val,
      googlesql::Value::MakeArray(StringArrayType(),
                                  {String("IsPrimaryUser"), String("UserId")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_type_arr_val,
      googlesql::Value::MakeArray(
          StringArrayType(),
          {String("{\"code\":\"BOOL\"}"), String("{\"code\":\"INT64\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_is_primary_key_arr_val,
      googlesql::Value::MakeArray(googlesql::types::BoolArrayType(),
                                  {Bool(true), Bool(false)}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_ordinal_position_arr_val,
      googlesql::Value::MakeArray(googlesql::types::Int64ArrayType(),
                                  {Int64(1), Int64(2)}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto mods_keys,
      googlesql::Value::MakeArray(StringArrayType(),
                                  {String("{\"IsPrimaryUser\":\"true\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto mods_new_values,
                       googlesql::Value::MakeArray(
                           StringArrayType(), {String("{\"UserId\":\"10\"}")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto mods_old_values,
      googlesql::Value::MakeArray(StringArrayType(), {String("{}")}));
  TestRowCursor cursor(
      {"partition_token", "commit_timestamp", "server_transaction_id",
       "record_sequence", "is_last_record_in_transaction_in_partition",
       "table_name", "column_types_name", "column_types_type",
       "column_type_is_primary_key", "column_types_ordinal_position",
       "mods_keys", "mods_new_values", "mods_old_values", "mod_type",
       "value_capture_type", "number_of_records_in_transaction",
       "number_of_partitions_in_transaction", "transaction_tag",
       "is_system_transaction"},
      {StringType(), TimestampType(), StringType(), StringType(), BoolType(),
       StringType(), StringType(), StringType(), BoolType(), Int64Type(),
       StringType(), StringType(), StringType(), StringType(), StringType(),
       Int64Type(), Int64Type(), StringType(), BoolType()},
      {{String("test_token"), Timestamp(now_), String("test_id"),
        String("00000001"), Bool(false), String("test_table"),
        col_types_name_arr_val, col_types_type_arr_val,
        col_types_is_primary_key_arr_val, col_types_ordinal_position_arr_val,
        mods_keys, mods_new_values, mods_old_values, String("UPDATE"),
        String("NEW_VALUES"), Int64(3), Int64(2), String("test_tag"),
        Bool(false)}});
  std::vector<PartialResultSet> results;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(results, ConvertDataTableRowCursorToBytes(
                                    &cursor,
                                    /*tvf_name=*/kDummyChangeStreamBytesTvf,
                                    /*expect_metadata=*/false));
  EXPECT_FALSE(results.empty());
  EXPECT_FALSE(results[0].has_metadata());
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
