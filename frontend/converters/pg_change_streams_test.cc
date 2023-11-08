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

#include <optional>
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
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
using zetasql::types::BoolType;
using zetasql::types::Int64Type;
using zetasql::types::StringArrayType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;
using zetasql::values::Bool;
using zetasql::values::Int64;
using zetasql::values::String;
using zetasql::values::Timestamp;
using testing::ElementsAre;
using test::EqualsProto;

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
};

TEST_F(PgChangeStreamResultConverterTest,
       PopulateFixedOutputColumnTypeMetadataForFirstResponse) {
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 1);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
  EXPECT_EQ(change_recods.heartbeat_records[0].timestamp.string_value(),
            absl::FormatTime(now_));
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertInitialPartitionTableRowCursorToMultipleChangeRecordsResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(StringArrayType(), {String("parent1")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val2,
      zetasql::Value::MakeArray(StringArrayType(), {String("parent2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val},
       {Timestamp(now_), String("token2"), parents_array_val2}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 2);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMoveEventPartitionTableRowCursorToChangeRecordResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(StringArrayType(), {String("move_token1")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(PgChangeStreamResultConverterTest,
       ConvertMergeEventPartitionTableRowCursorToChangeRecordResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(
          StringArrayType(), {String("merge_token1"), String("merge_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToJson(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/false));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(StringArrayType(), {String("parent_token")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("split_token1"), parents_array_val},
       {Timestamp(now_), String("split_token2"), parents_array_val}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToJson(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*tvf_name=*/kDummyChangeStreamJsonTvf,
                           /*expect_metadata=*/false));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_name_arr_val,
      zetasql::Value::MakeArray(StringArrayType(),
                                  {String("IsPrimaryUser"), String("UserId")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_type_arr_val,
      zetasql::Value::MakeArray(
          StringArrayType(),
          {String("{\"code\":\"BOOL\"}"), String("{\"code\":\"STRING\"}")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_is_primary_key_arr_val,
      zetasql::Value::MakeArray(zetasql::types::BoolArrayType(),
                                  {Bool(false), Bool(true)}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_ordinal_position_arr_val,
      zetasql::Value::MakeArray(zetasql::types::Int64ArrayType(),
                                  {Int64(1), Int64(2)}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto mods_keys,
      zetasql::Value::MakeArray(StringArrayType(),
                                  {String("{\"UserId\": \"User2\"}"),
                                   String("{\"UserId\": \"User2\"}")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto mods_new_values,
      zetasql::Value::MakeArray(
          StringArrayType(),
          {String("{\"IsPrimaryUser\": true,\"UserId\": \"User2\"}"),
           String("{\"IsPrimaryUser\": false}")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto mods_old_values,
                       zetasql::Value::MakeArray(
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 1);
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
