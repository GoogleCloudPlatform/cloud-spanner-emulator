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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/options.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/options.h"
#include "google/cloud/spanner/transaction.h"
#include "common/clock.h"
#include "tests/common/change_streams.h"
#include "tests/conformance/common/database_test_base.h"
#include "tests/conformance/common/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {
using googlesql_base::testing::IsOkAndHolds;

class ChangeStreamExclusionTxnTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    if (dialect_ == database_api::POSTGRESQL) {
      GOOGLESQL_RETURN_IF_ERROR(SetSchema({
          R"(
          CREATE TABLE WatchedTable (
            Id       bigint NOT NULL,
            Name     varchar,
            Tag      varchar,
            PRIMARY KEY (Id)
          )
        )",
          R"(
          CREATE CHANGE STREAM ExcludedStream FOR WatchedTable WITH (allow_txn_exclusion=true)
        )",
          R"(
          CREATE CHANGE STREAM IncludedStream FOR WatchedTable WITH (allow_txn_exclusion=false)
        )"}));
    } else {
      GOOGLESQL_RETURN_IF_ERROR(SetSchema({
          R"(
          CREATE TABLE WatchedTable (
            Id       INT64 NOT NULL,
            Name     STRING(MAX),
            Tag      STRING(MAX),
          ) PRIMARY KEY (Id)
        )",
          R"(
          CREATE CHANGE STREAM ExcludedStream FOR WatchedTable OPTIONS (allow_txn_exclusion=true)
        )",
          R"(
          CREATE CHANGE STREAM IncludedStream FOR WatchedTable OPTIONS (allow_txn_exclusion=false)
        )"}));
    }
    GOOGLESQL_ASSIGN_OR_RETURN(test_session_uri_,
                     CreateTestSession(raw_client(), database()));
    return absl::OkStatus();
  }

 protected:
  std::string test_session_uri_;
  const std::string excluded_stream_ = "ExcludedStream";
  const std::string included_stream_ = "IncludedStream";

  absl::StatusOr<int> CountDataRecords(absl::Time start,
                                       std::string change_stream_name) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        std::vector<std::string> active_tokens,
        GetActiveTokenFromInitialQuery(dialect_, start, change_stream_name,
                                       test_session_uri_, raw_client()));
    int data_records_count = 0;
    for (const auto& partition_token : active_tokens) {
      std::string sql_template =
          "SELECT * FROM READ_$0 ('$1', '$2', '$3', 300000)";
      if (dialect_ == database_api::POSTGRESQL) {
        sql_template =
            "SELECT * FROM spanner.read_json_$0 ('$1', '$2', '$3', 300000)";
      }
      std::string sql = absl::Substitute(sql_template, change_stream_name,
                                         start, Clock().Now(), partition_token);
      GOOGLESQL_ASSIGN_OR_RETURN(
          test::ChangeStreamRecords change_records,
          ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
      data_records_count += change_records.data_change_records.size();
    }
    return data_records_count;
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectChangeStreamExclusionTxnTest, ChangeStreamExclusionTxnTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ChangeStreamExclusionTxnTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(ChangeStreamExclusionTxnTest, ExecuteDML) {
  absl::Time start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                true));
    GOOGLESQL_EXPECT_OK(CommitDml(
        {SqlStatement("INSERT INTO WatchedTable (Id, Name, Tag) VALUES (1, "
                      "'name1', 'tag1')")}));
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(0));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));

  start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                false));
    GOOGLESQL_EXPECT_OK(
        CommitDml({SqlStatement("DELETE FROM WatchedTable WHERE Id = 1")}));
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(1));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));
}

TEST_P(ChangeStreamExclusionTxnTest, ExecuteBatchDML) {
  absl::Time start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                true));
    GOOGLESQL_EXPECT_OK(CommitBatchDml(
        {SqlStatement("INSERT INTO WatchedTable (Id, Name, Tag) VALUES (1, "
                      "'name1', 'tag1')"),
         SqlStatement("INSERT INTO WatchedTable (Id, Name, Tag) VALUES (2, "
                      "'name2', 'tag2')"),
         SqlStatement("UPDATE WatchedTable SET Name = 'NAME1' WHERE Id = 1")}));
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(0));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_),
              IsOkAndHolds(GetConformanceTestGlobals().in_prod_env ? 2 : 1));
}

TEST_P(ChangeStreamExclusionTxnTest, TurnOffAllowTxnExclusion_ExpectRecords) {
  if (dialect_ == database_api::POSTGRESQL) {
    GOOGLESQL_EXPECT_OK(
        UpdateSchema({"ALTER CHANGE STREAM ExcludedStream SET "
                      "(allow_txn_exclusion=false)"}));
  } else {
    GOOGLESQL_EXPECT_OK(
        UpdateSchema({"ALTER CHANGE STREAM ExcludedStream SET OPTIONS "
                      "(allow_txn_exclusion=false)"}));
  }
  absl::Time start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                true));
    GOOGLESQL_EXPECT_OK(CommitDml(
        {SqlStatement("INSERT INTO WatchedTable (Id, Name, Tag) VALUES (1, "
                      "'name1', 'tag1')")}));
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(1));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));
}

TEST_P(ChangeStreamExclusionTxnTest, BlindWrite) {
  absl::Time start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                true));
    GOOGLESQL_EXPECT_OK(
        Insert("WatchedTable", {"Id", "Name", "Tag"}, {1, "name1", "tag1"}));
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(0));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));

  start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                false));
    GOOGLESQL_EXPECT_OK(
        Insert("WatchedTable", {"Id", "Name", "Tag"}, {2, "name2", "tag2"}));
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(1));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));
}

TEST_P(ChangeStreamExclusionTxnTest, BatchWrite) {
  absl::Time start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                true));
    BatchWrite({Mutations{cloud::spanner::InsertMutationBuilder(
                              "WatchedTable", {"Id", "Name", "Tag"})
                              .AddRow(ValueRow{1, "name1", "tag1"})
                              .Build()}});
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(0));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));

  start_ts = Clock().Now();
  {
    cloud::internal::OptionsSpan span(
        cloud::Options{}
            .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
                false));
    BatchWrite({Mutations{cloud::spanner::InsertMutationBuilder(
                              "WatchedTable", {"Id", "Name", "Tag"})
                              .AddRow(ValueRow{2, "name2", "tag2"})
                              .Build()}});
  }
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(1));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));
}

TEST_P(ChangeStreamExclusionTxnTest, ReadThenWriteMutationsInOneTxn) {
  absl::Time start_ts = Clock().Now();
  cloud::internal::OptionsSpan span(
      cloud::Options{}
          .set<cloud::spanner::ExcludeTransactionFromChangeStreamsOption>(
              true));
  GOOGLESQL_EXPECT_OK(CommitTransaction(cloud::spanner::MakeReadWriteTransaction(),
                              {cloud::spanner::InsertMutationBuilder(
                                   "WatchedTable", {"Id", "Name", "Tag"})
                                   .AddRow(ValueRow{1, "name1", "tag1"})
                                   .Build()}));
  EXPECT_THAT(CountDataRecords(start_ts, excluded_stream_), IsOkAndHolds(0));
  EXPECT_THAT(CountDataRecords(start_ts, included_stream_), IsOkAndHolds(1));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
