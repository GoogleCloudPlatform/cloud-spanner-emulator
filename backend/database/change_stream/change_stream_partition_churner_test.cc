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

#include "backend/database/change_stream/change_stream_partition_churner.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/database/database.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "common/clock.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class ChangeStreamPartitionChurnerTest : public ::testing::Test {
 protected:
  struct TestChurnedPartitions {
    std::string partition_token;
    absl::Time start_time;
    absl::Time end_time;
    std::string change_stream_name;
    std::vector<std::string> parents;
    std::vector<std::string> children;
  };

  struct StaleAndActivePartitions {
    std::vector<TestChurnedPartitions> stale_partitions;
    std::vector<TestChurnedPartitions> active_partitions;
  };

  void SetUp() override {
    absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Seconds(1));
    absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                  absl::Seconds(1));
    std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )",
                                                  R"(
    CREATE CHANGE STREAM change_stream_one FOR ALL
  )"};
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        db_, Database::Create(&clock_, SchemaChangeOperation{
                                           .statements = create_statements}));
  }

  void AddChangeStream(absl::string_view change_stream_name) {
    std::string create_change_stream =
        "CREATE CHANGE STREAM " + std::string(change_stream_name);
    std::vector<std::string> update_statements = {create_change_stream};

    absl::Status backfill_status;
    int completed_statements;
    absl::Time commit_ts;
    absl::Status s;
    do {
      s = db_->UpdateSchema(
          SchemaChangeOperation{.statements = update_statements},
          &completed_statements, &commit_ts, &backfill_status);
    } while (!s.ok());
  }

  void DropChangeStream(absl::string_view change_stream_name) {
    std::string create_change_stream =
        "DROP CHANGE STREAM " + std::string(change_stream_name);
    std::vector<std::string> update_statements = {create_change_stream};

    absl::Status backfill_status;
    int completed_statements;
    absl::Time commit_ts;
    absl::Status s;
    do {
      s = db_->UpdateSchema(
          SchemaChangeOperation{.statements = update_statements},
          &completed_statements, &commit_ts, &backfill_status);
    } while (!s.ok());
  }

  absl::StatusOr<TestChurnedPartitions> GetPartition(
      StaleAndActivePartitions partitions, std::string partition_token) {
    for (const auto& partition : partitions.active_partitions) {
      if (partition.partition_token == partition_token) {
        return partition;
      }
    }
    for (const auto& partition : partitions.stale_partitions) {
      if (partition.partition_token == partition_token) {
        return partition;
      }
    }
    return absl::NotFoundError(partition_token);
  }
  absl::StatusOr<StaleAndActivePartitions> GetChangeStreamPartitions(
      std::string change_stream_name, Database* db) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ReadOnlyTransaction> txn,
                     db->CreateReadOnlyTransaction(ReadOnlyOptions()));
    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.change_stream_for_partition_table = change_stream_name;
    read_arg.columns = {"partition_token", "start_time", "end_time", "parents",
                        "children"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;

    std::vector<TestChurnedPartitions> stale_partitions;
    std::vector<TestChurnedPartitions> active_partitions;
    while (cursor->Next()) {
      const std::string& partitionToken = cursor->ColumnValue(0).string_value();
      const absl::Time start_time = cursor->ColumnValue(1).ToTime();
      std::vector<std::string> parents;
      if (!cursor->ColumnValue(3).is_null()) {
        const std::vector<zetasql::Value>& parents_cursor =
            cursor->ColumnValue(3).elements();

        for (const auto& parent : parents_cursor) {
          if (parent.is_null()) {
            continue;
          }
          parents.push_back(parent.string_value());
        }
      }
      std::vector<std::string> children;
      if (!cursor->ColumnValue(4).is_null()) {
        const std::vector<zetasql::Value>& children_cursor =
            cursor->ColumnValue(4).elements();

        for (const auto& child : children_cursor) {
          if (child.is_null()) {
            continue;
          }
          children.push_back(child.string_value());
        }
      }

      if (!cursor->ColumnValue(2).is_null()) {
        stale_partitions.push_back({partitionToken, start_time,
                                    cursor->ColumnValue(2).ToTime(),
                                    change_stream_name, parents, children});
      } else {
        active_partitions.push_back({partitionToken, start_time,
                                     absl::InfinitePast(), change_stream_name,
                                     parents, children});
      }
    }
    return StaleAndActivePartitions{.stale_partitions = stale_partitions,
                                    .active_partitions = active_partitions};
  }

  // This function verifies that each stale partition has 1 child. For each
  // child, we verify that it has the same start time as its parent's end
  // timestamp. We verify that the child has the parent in its parents list, and
  // that the parent has the child in its children list.
  void VerifyStaleAndActivePartitions(
      StaleAndActivePartitions stale_and_active_partitions) {
    // Verify the content of stale and active partitions.
    for (const auto& stale_token :
         stale_and_active_partitions.stale_partitions) {
      // Verify that each stale token has 1 child.
      std::vector<std::string> children = stale_token.children;
      ASSERT_EQ(1, children.size());

      // Get the child token for the stale token.
      ZETASQL_ASSERT_OK_AND_ASSIGN(
          TestChurnedPartitions child_partition,
          GetPartition(stale_and_active_partitions, children[0]));
      // Verify that the child's start time and the stale token's end time
      // is the same.
      ASSERT_EQ(child_partition.start_time, stale_token.end_time);

      // Verify that the child token's parents is the same as the stale
      // token.
      std::vector<std::string> expected_parents = {stale_token.partition_token};
      ASSERT_EQ(child_partition.parents, expected_parents);
    }
  }

  Clock clock_;
  std::unique_ptr<Database> db_;
};

TEST_F(ChangeStreamPartitionChurnerTest, ChangeStreamChurning) {
  std::string change_stream_one = "change_stream_one";
  ASSERT_EQ(1, db_->get_change_stream_partition_churner()->GetNumThreads());

  absl::SleepFor(
      absl::GetFlag(FLAGS_change_stream_churn_thread_sleep_interval) * 5);

  ZETASQL_ASSERT_OK_AND_ASSIGN(StaleAndActivePartitions stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_one, db_.get()));
  // We expect that there are at least two stale partitions for the existing
  // change stream, since we have slept for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 2);
  // With move churning, there should only ever be 2 active tokens.
  EXPECT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  std::string change_stream_two = "change_stream_two";
  AddChangeStream(change_stream_two);
  ASSERT_EQ(2, db_->get_change_stream_partition_churner()->GetNumThreads());

  absl::SleepFor(
      absl::GetFlag(FLAGS_change_stream_churn_thread_sleep_interval) * 5);

  ZETASQL_ASSERT_OK_AND_ASSIGN(stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_one, db_.get()));
  // We expect that there are at least four stale partitions for the existing
  // change stream, since we have slept again for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 4);
  // With move churning, there should only ever be 2 active tokens.
  EXPECT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  ZETASQL_ASSERT_OK_AND_ASSIGN(stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_two, db_.get()));
  // We expect that there are at least two stale partitions for the newly
  // added change stream, since we have slept for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 2);
  // With move churning, there should only ever be 2 active tokens.
  EXPECT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  std::string change_stream_three = "change_stream_three";
  AddChangeStream(change_stream_three);
  ASSERT_EQ(3, db_->get_change_stream_partition_churner()->GetNumThreads());

  absl::SleepFor(absl::Seconds(5));

  ZETASQL_ASSERT_OK_AND_ASSIGN(stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_one, db_.get()));
  // We expect that there are at least six stale partitions for the existing
  // change_stream_one, since we have slept for 5x the current churn
  // interval for the third time.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 6);
  // With move churning, there should only ever be 2 active tokens.
  EXPECT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  ZETASQL_ASSERT_OK_AND_ASSIGN(stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_two, db_.get()));
  // We expect that there are at least four stale partitions for the existing
  // change stream, since we have slept again for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 4);
  // With move churning, there should only ever be 2 active tokens.
  EXPECT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      stale_and_active_partitions,
      GetChangeStreamPartitions(change_stream_three, db_.get()));
  // We expect that there are at least two stale partitions for the newly
  // added change stream, since we have slept for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 2);
  // With move churning, there should only ever be 2 active tokens.
  EXPECT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  DropChangeStream(change_stream_three);
  ASSERT_EQ(2, db_->get_change_stream_partition_churner()->GetNumThreads());

  DropChangeStream(change_stream_two);
  ASSERT_EQ(1, db_->get_change_stream_partition_churner()->GetNumThreads());

  DropChangeStream(change_stream_one);
  ASSERT_EQ(0, db_->get_change_stream_partition_churner()->GetNumThreads());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
