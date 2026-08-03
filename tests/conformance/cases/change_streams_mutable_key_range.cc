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

#include <cstdint>
#include <string>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/timestamp.h"
#include "common/clock.h"
#include "tests/common/change_streams.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "googlesql/base/status_macros.h"
#include "grpcpp/client_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using InsertMutationBuilder = cloud::spanner::InsertMutationBuilder;

class ChangeStreamMutableKeyRangeTest : public DatabaseTest {
 protected:
  ChangeStreamMutableKeyRangeTest()
      : feature_flags_({.enable_mutable_key_range_change_stream = true}) {}

  void SetUp() override { DatabaseTest::SetUp(); }

  absl::Status SetUpDatabase() override {
    GOOGLESQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            UserId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (UserId)
        )",
        R"(
          CREATE CHANGE STREAM StreamUsers FOR Users
            OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')
        )",
    }));
    GOOGLESQL_ASSIGN_OR_RETURN(test_session_uri_, CreateTestSession());
    return absl::OkStatus();
  }

  ScopedEmulatorFeatureFlagsSetter feature_flags_;
  std::string test_session_uri_;

  absl::StatusOr<std::string> CreateTestSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    spanner_api::Session response;
    request.set_database(database()->FullName());
    GOOGLESQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response.name();
  }

  absl::StatusOr<std::vector<std::string>> GetActiveTokens(absl::Time start,
                                                           absl::Time end) {
    std::string sql = absl::Substitute(
        "SELECT * FROM READ_StreamUsers('$0', '$1', NULL, 300000)", start, end);
    GOOGLESQL_ASSIGN_OR_RETURN(
        test::ChangeStreamRecords change_records,
        ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
    std::vector<std::string> tokens;
    for (const auto& record : change_records.partition_start_records) {
      for (const auto& token : record.partition_tokens()) {
        tokens.push_back(token);
      }
    }
    return tokens;
  }

  absl::StatusOr<
      std::vector<::google::spanner::v1::ChangeStreamRecord::DataChangeRecord>>
  GetDataRecords(absl::Time start, absl::Time end) {
    GOOGLESQL_ASSIGN_OR_RETURN(auto active_tokens, GetActiveTokens(start, end));
    std::vector<::google::spanner::v1::ChangeStreamRecord::DataChangeRecord>
        data_records;
    for (const auto& token : active_tokens) {
      std::string sql = absl::Substitute(
          "SELECT * FROM READ_StreamUsers('$0', '$1', '$2', 300000)", start,
          end, token);
      GOOGLESQL_ASSIGN_OR_RETURN(
          test::ChangeStreamRecords change_records,
          ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
      data_records.insert(
          data_records.end(),
          change_records.mutable_key_range_data_change_records.begin(),
          change_records.mutable_key_range_data_change_records.end());
    }
    return data_records;
  }
};

TEST_F(ChangeStreamMutableKeyRangeTest,
       InitialQueryReturnsPartitionStartRecord) {
  absl::Time start = Clock().Now();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto tokens, GetActiveTokens(start, start));
  EXPECT_FALSE(tokens.empty());
}

TEST_F(ChangeStreamMutableKeyRangeTest,
       SingleInsertVerifyMutableKeyRangeRecord) {
  absl::Time query_start_time = Clock().Now();

  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  mutation_builder_insert.EmplaceRow(int64_t{1}, "Alice", int64_t{30});
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));

  absl::Time commit_ts =
      commit_result.commit_timestamp.get<absl::Time>().value();

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecords(query_start_time, commit_ts));
  ASSERT_FALSE(data_change_records.empty());
  const auto& record = data_change_records[0];

  // Verify commit_timestamp and server_transaction_id non-empty
  EXPECT_TRUE(record.has_commit_timestamp());
  EXPECT_EQ(record.commit_timestamp().seconds(),
            absl::ToUnixSeconds(commit_ts));
  EXPECT_FALSE(record.server_transaction_id().empty());
  EXPECT_THAT(record.record_sequence(),
              testing::MatchesRegex("(?:[0-9a-f]{16}-)?00000000"));

  // Verify entire proto content using EqualsProto matcher
  EXPECT_THAT(record, test::proto::IgnoringRepeatedFieldOrdering(
                          test::proto::Partially(test::EqualsProto(R"pb(
                            table: "Users"
                            mod_type: INSERT
                            value_capture_type: OLD_AND_NEW_VALUES
                            is_last_record_in_transaction_in_partition: true
                            number_of_records_in_transaction: 1
                            number_of_partitions_in_transaction: 1
                            transaction_tag: ""
                            is_system_transaction: false
                            column_metadata {
                              name: "UserId"
                              type { code: INT64 }
                              is_primary_key: true
                              ordinal_position: 1
                            }
                            column_metadata {
                              name: "Name"
                              type { code: STRING }
                              is_primary_key: false
                              ordinal_position: 2
                            }
                            column_metadata {
                              name: "Age"
                              type { code: INT64 }
                              is_primary_key: false
                              ordinal_position: 3
                            }
                            mods {
                              keys {
                                column_metadata_index: 0
                                value { string_value: "1" }
                              }
                              new_values {
                                column_metadata_index: 1
                                value { string_value: "Alice" }
                              }
                              new_values {
                                column_metadata_index: 2
                                value { string_value: "30" }
                              }
                            }
                          )pb"))));
}

class PGChangeStreamMutableKeyRangeTest : public DatabaseTest {
 protected:
  PGChangeStreamMutableKeyRangeTest()
      : feature_flags_({.enable_postgresql_interface = true,
                        .enable_mutable_key_range_change_stream = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    GOOGLESQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE users (
            user_id bigint NOT NULL PRIMARY KEY,
            name varchar,
            age bigint
          );
        )",
        R"(
          CREATE CHANGE STREAM stream_users FOR users
            WITH (partition_mode = 'MUTABLE_KEY_RANGE');
        )",
    }));
    GOOGLESQL_ASSIGN_OR_RETURN(test_session_uri_, CreateTestSession());
    return absl::OkStatus();
  }

  ScopedEmulatorFeatureFlagsSetter feature_flags_;
  std::string test_session_uri_;

  absl::StatusOr<std::string> CreateTestSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    spanner_api::Session response;
    request.set_database(database()->FullName());
    GOOGLESQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response.name();
  }

  absl::StatusOr<std::vector<std::string>> GetActiveTokens(absl::Time start,
                                                           absl::Time end) {
    std::string sql = absl::Substitute(
        "SELECT * FROM "
        "spanner.read_proto_bytes_stream_users('$0'::timestamptz, "
        "'$1'::timestamptz, NULL, 300000, NULL::text[])",
        start, end);
    GOOGLESQL_ASSIGN_OR_RETURN(
        test::ChangeStreamRecords change_records,
        ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
    std::vector<std::string> tokens;
    for (const auto& record : change_records.partition_start_records) {
      for (const auto& token : record.partition_tokens()) {
        tokens.push_back(token);
      }
    }
    return tokens;
  }

  absl::StatusOr<
      std::vector<::google::spanner::v1::ChangeStreamRecord::DataChangeRecord>>
  GetDataRecords(absl::Time start, absl::Time end) {
    GOOGLESQL_ASSIGN_OR_RETURN(auto active_tokens, GetActiveTokens(start, end));
    std::vector<::google::spanner::v1::ChangeStreamRecord::DataChangeRecord>
        data_records;
    for (const auto& token : active_tokens) {
      std::string sql = absl::Substitute(
          "SELECT * FROM "
          "spanner.read_proto_bytes_stream_users('$0'::timestamptz, "
          "'$1'::timestamptz, '$2', 300000, NULL::text[])",
          start, end, token);
      GOOGLESQL_ASSIGN_OR_RETURN(
          test::ChangeStreamRecords change_records,
          ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
      data_records.insert(
          data_records.end(),
          change_records.mutable_key_range_data_change_records.begin(),
          change_records.mutable_key_range_data_change_records.end());
    }
    return data_records;
  }
};

TEST_F(PGChangeStreamMutableKeyRangeTest,
       InitialQueryReturnsPartitionStartRecord) {
  absl::Time start = Clock().Now();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto tokens, GetActiveTokens(start, start));
  EXPECT_FALSE(tokens.empty());
}

TEST_F(PGChangeStreamMutableKeyRangeTest,
       SingleInsertVerifyMutableKeyRangeRecord) {
  absl::Time query_start_time = Clock().Now();

  auto mutation_builder_insert =
      InsertMutationBuilder("users", {"user_id", "name", "age"});
  mutation_builder_insert.EmplaceRow(int64_t{1}, "Bob", int64_t{25});
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));

  absl::Time commit_ts =
      commit_result.commit_timestamp.get<absl::Time>().value();

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecords(query_start_time, commit_ts));
  ASSERT_FALSE(data_change_records.empty());
  const auto& record = data_change_records[0];

  // Verify commit_timestamp and server_transaction_id non-empty
  EXPECT_TRUE(record.has_commit_timestamp());
  EXPECT_EQ(record.commit_timestamp().seconds(),
            absl::ToUnixSeconds(commit_ts));
  EXPECT_FALSE(record.server_transaction_id().empty());
  EXPECT_THAT(record.record_sequence(),
              testing::MatchesRegex("(?:[0-9a-f]{16}-)?00000000"));

  // Verify entire proto content using EqualsProto matcher
  EXPECT_THAT(record, test::proto::IgnoringRepeatedFieldOrdering(
                          test::proto::Partially(test::EqualsProto(R"pb(
                            table: "users"
                            mod_type: INSERT
                            value_capture_type: OLD_AND_NEW_VALUES
                            is_last_record_in_transaction_in_partition: true
                            number_of_records_in_transaction: 1
                            number_of_partitions_in_transaction: 1
                            transaction_tag: ""
                            is_system_transaction: false
                            column_metadata {
                              name: "user_id"
                              type { code: INT64 }
                              is_primary_key: true
                              ordinal_position: 1
                            }
                            column_metadata {
                              name: "name"
                              type { code: STRING }
                              is_primary_key: false
                              ordinal_position: 2
                            }
                            column_metadata {
                              name: "age"
                              type { code: INT64 }
                              is_primary_key: false
                              ordinal_position: 3
                            }
                            mods {
                              keys {
                                column_metadata_index: 0
                                value { string_value: "1" }
                              }
                              new_values {
                                column_metadata_index: 1
                                value { string_value: "Bob" }
                              }
                              new_values {
                                column_metadata_index: 2
                                value { string_value: "25" }
                              }
                            }
                          )pb"))));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
