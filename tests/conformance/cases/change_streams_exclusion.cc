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

#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/keys.h"
#include "google/cloud/spanner/mutations.h"
#include "common/clock.h"
#include "tests/common/change_streams.h"
#include "tests/conformance/common/database_test_base.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {
using InsertMutationBuilder = cloud::spanner::InsertMutationBuilder;
using UpdateMutationBuilder = cloud::spanner::UpdateMutationBuilder;
using DeleteMutationBuilder = cloud::spanner::DeleteMutationBuilder;
using ReplaceMutationBuilder = cloud::spanner::ReplaceMutationBuilder;
class ChangeStreamExclusionTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchemaFromFile("change_streams_exclusion.test"));
    ZETASQL_ASSIGN_OR_RETURN(test_session_uri_,
                     CreateTestSession(raw_client(), database()));
    ZETASQL_RETURN_IF_ERROR(PopulateTestData());
    return absl::OkStatus();
  }

 protected:
  std::string test_session_uri_;
  const std::string exclude_insert_change_stream_ = "Stream1";
  const std::string exclude_update_change_stream_ = "Stream2";
  const std::string exclude_delete_change_stream_ = "Stream3";

  struct DataChangeRecordsCount {
    int insert_count = 0;
    int update_count = 0;
    int delete_count = 0;
    int total_count = 0;
  };

  absl::Status PopulateTestData() {
    auto mutation_builder_insert =
        InsertMutationBuilder("Users", {"UserId", "Name"});
    std::vector<ValueRow> rows = {{1, "name1"}, {2, "name2"}};
    for (const auto& row : rows) {
      mutation_builder_insert.AddRow(row);
    }
    return Commit({mutation_builder_insert.Build()}).status();
  }

  absl::StatusOr<DataChangeRecordsCount> CountDataRecordsFromStartToNow(
      absl::Time start, std::string change_stream_name) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::string> active_tokens,
        GetActiveTokenFromInitialQuery(dialect_, start, change_stream_name,
                                       test_session_uri_, raw_client()));
    std::vector<DataChangeRecord> merged_data_change_records;
    DataChangeRecordsCount data_change_records_count;
    for (const auto& partition_token : active_tokens) {
      std::string sql_template =
          "SELECT * FROM READ_$0 ('$1', '$2', '$3', 300000)";
      if (dialect_ == database_api::POSTGRESQL) {
        sql_template =
            "SELECT * FROM spanner.read_json_$0 ('$1', '$2', '$3', 300000)";
      }
      std::string sql = absl::Substitute(sql_template, change_stream_name,
                                         start, Clock().Now(), partition_token);
      ZETASQL_ASSIGN_OR_RETURN(
          test::ChangeStreamRecords change_records,
          ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
      for (const auto& data_change_record :
           change_records.data_change_records) {
        if (data_change_record.mod_type.string_value() == "INSERT") {
          data_change_records_count.insert_count++;
        } else if (data_change_record.mod_type.string_value() == "UPDATE") {
          data_change_records_count.update_count++;
        } else if (data_change_record.mod_type.string_value() == "DELETE") {
          data_change_records_count.delete_count++;
        }
      }
    }

    data_change_records_count.total_count =
        data_change_records_count.insert_count +
        data_change_records_count.update_count +
        data_change_records_count.delete_count;
    return data_change_records_count;
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectChangeStreamExclusionTest, ChangeStreamExclusionTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ChangeStreamExclusionTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(ChangeStreamExclusionTest, SingleInsertModTypeFilter) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{3, "name3"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_insert_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 0);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_update_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 1);
  ASSERT_EQ(data_change_records.insert_count, 1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_delete_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 1);
  ASSERT_EQ(data_change_records.insert_count, 1);
}

TEST_P(ChangeStreamExclusionTest, SingleUpdateModTypeFilter) {
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_insert_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 1);
  ASSERT_EQ(data_change_records.update_count, 1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_update_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 0);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_delete_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 1);
  ASSERT_EQ(data_change_records.update_count, 1);
}

TEST_P(ChangeStreamExclusionTest, SingleDeleteModTypeFilter) {
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_insert_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 1);
  ASSERT_EQ(data_change_records.delete_count, 1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_update_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 1);
  ASSERT_EQ(data_change_records.delete_count, 1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_delete_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 0);
}

TEST_P(ChangeStreamExclusionTest, MultipleModTypesFilter) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{3, "name3"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }

  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }

  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(2)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto commit_result,
      Commit({mutation_builder_insert.Build(), mutation_builder_update.Build(),
              mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_insert_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 2);
  ASSERT_EQ(data_change_records.insert_count, 0);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_update_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 2);
  ASSERT_EQ(data_change_records.update_count, 0);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_delete_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 2);
  ASSERT_EQ(data_change_records.delete_count, 0);
}

TEST_P(ChangeStreamExclusionTest, CascadeDeleteModTypeFilter) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Accounts", {"UserId", "AccountId", "Balance"});
  std::vector<ValueRow> rows = {{1, 111, 250}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK(Commit({mutation_builder_insert.Build()}));

  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_insert_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 2);
  ASSERT_EQ(data_change_records.delete_count, 2);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_update_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 2);
  ASSERT_EQ(data_change_records.delete_count, 2);

  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       CountDataRecordsFromStartToNow(
                           query_start_time, exclude_delete_change_stream_));
  ASSERT_EQ(data_change_records.total_count, 0);
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
