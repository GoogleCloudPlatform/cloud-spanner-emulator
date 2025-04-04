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

constexpr char kDatabaseId[] = "test-db";

class ChangeStreamPartitionChurnerTest : public ::testing::Test {
 protected:
  struct TestChurnedPartitions {
    std::string partition_token;
    absl::Time start_time;
    absl::Time end_time;
    std::string change_stream_name;
    std::vector<std::string> parents;
    std::vector<std::string> children;
    std::string next_churn;
  };

  struct StaleAndActivePartitions {
    std::vector<TestChurnedPartitions> stale_partitions;
    std::vector<TestChurnedPartitions> active_partitions;
  };

  void SetUp() override {
    absl::SetFlag(&FLAGS_override_change_stream_partition_token_alive_seconds,
                  1);
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
        db_, Database::Create(
                 &clock_, kDatabaseId,
                 SchemaChangeOperation{.statements = create_statements}));
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
    read_arg.columns = {"partition_token", "start_time", "end_time",
                        "parents",         "children",   "next_churn"};
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
      const std::string& next_churn = cursor->ColumnValue(5).string_value();

      if (!cursor->ColumnValue(2).is_null()) {
        stale_partitions.push_back(
            {partitionToken, start_time, cursor->ColumnValue(2).ToTime(),
             change_stream_name, parents, children, next_churn});
      } else {
        active_partitions.push_back({partitionToken, start_time,
                                     absl::InfinitePast(), change_stream_name,
                                     parents, children, next_churn});
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

    std::vector<TestChurnedPartitions> all_partitions;
    for (const auto& stale_token :
         stale_and_active_partitions.stale_partitions) {
      all_partitions.push_back(stale_token);
    }
    for (const auto& active_token :
         stale_and_active_partitions.active_partitions) {
      all_partitions.push_back(active_token);
    }
    for (const auto& token : all_partitions) {
      ASSERT_TRUE(!token.children.empty() || !token.parents.empty());
      for (const auto& child : token.children) {
        // Get the child token for the stale token.
        ZETASQL_ASSERT_OK_AND_ASSIGN(TestChurnedPartitions child_partition,
                             GetPartition(stale_and_active_partitions, child));
        // Verify that the child's start time and the stale token's end time
        // is the same.
        ASSERT_EQ(child_partition.start_time, token.end_time);
        ASSERT_THAT(child_partition.parents,
                    testing::Contains(token.partition_token));
        // for each of the child's parents, verify that the parent exists and
        // contains the child in its childrens list.
        for (const auto& parent : child_partition.parents) {
          ZETASQL_ASSERT_OK_AND_ASSIGN(
              TestChurnedPartitions parent_partition,
              GetPartition(stale_and_active_partitions, parent));
          // Verify that each parent contains the correct information.
          ASSERT_THAT(parent_partition.children, testing::Contains(child));
          ASSERT_EQ(parent_partition.end_time, child_partition.start_time);
        }
      }
      // Verify that the child token's parents contains the stale token
      for (const auto& parent : token.parents) {
        ZETASQL_ASSERT_OK_AND_ASSIGN(TestChurnedPartitions parent_partition,
                             GetPartition(stale_and_active_partitions, parent));
        // Verify that each parent contains the correct information.
        ASSERT_THAT(parent_partition.children,
                    testing::Contains(token.partition_token));
        ASSERT_EQ(parent_partition.end_time, token.start_time);
        // For each parent, go through it's children's list and verify that
        // the child contains the parent in the parent's list.
        for (const auto& child : parent_partition.children) {
          ZETASQL_ASSERT_OK_AND_ASSIGN(
              TestChurnedPartitions child_partition,
              GetPartition(stale_and_active_partitions, child));
          // Verify that each parent contains the correct information.
          ASSERT_THAT(child_partition.parents, testing::Contains(parent));
          ASSERT_EQ(child_partition.start_time, parent_partition.end_time);
        }
      }
    }
  }

  void ChurnPartitionsForChangeStream(std::string change_stream_name) {
    absl::Status s;
    do {
      s = db_->get_change_stream_partition_churner()->ChurnPartitions(
          change_stream_name);
    } while (!s.ok());
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
  // With churning, there should always at least be two active tokens.
  EXPECT_GE(stale_and_active_partitions.active_partitions.size(), 2);
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
  // With churning, there should always at least be two active tokens.
  EXPECT_GE(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  ZETASQL_ASSERT_OK_AND_ASSIGN(stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_two, db_.get()));
  // We expect that there are at least two stale partitions for the newly
  // added change stream, since we have slept for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 2);
  // With churning, there should always at least be two tokens.
  EXPECT_GE(stale_and_active_partitions.active_partitions.size(), 2);
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
  // With churning, there should always at least be two tokens.
  EXPECT_GE(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  ZETASQL_ASSERT_OK_AND_ASSIGN(stale_and_active_partitions,
                       GetChangeStreamPartitions(change_stream_two, db_.get()));
  // We expect that there are at least four stale partitions for the existing
  // change stream, since we have slept again for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 4);
  // With churning, there should always at least be two tokens.
  EXPECT_GE(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      stale_and_active_partitions,
      GetChangeStreamPartitions(change_stream_three, db_.get()));
  // We expect that there are at least two stale partitions for the newly
  // added change stream, since we have slept for 5x the current churn interval.
  EXPECT_GT(stale_and_active_partitions.stale_partitions.size(), 2);
  // With churning, there should always at least be two tokens.
  EXPECT_GE(stale_and_active_partitions.active_partitions.size(), 2);
  VerifyStaleAndActivePartitions(stale_and_active_partitions);

  DropChangeStream(change_stream_three);
  ASSERT_EQ(2, db_->get_change_stream_partition_churner()->GetNumThreads());

  DropChangeStream(change_stream_two);
  ASSERT_EQ(1, db_->get_change_stream_partition_churner()->GetNumThreads());

  DropChangeStream(change_stream_one);
  ASSERT_EQ(0, db_->get_change_stream_partition_churner()->GetNumThreads());
}

TEST_F(ChangeStreamPartitionChurnerTest, ChangeStreamSplitAndMerge) {
  absl::SetFlag(&FLAGS_enable_change_stream_churning, false);
  std::string change_stream_disable_churning = "change_stream_disable_churning";
  AddChangeStream(change_stream_disable_churning);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      StaleAndActivePartitions stale_and_active_partitions,
      GetChangeStreamPartitions(change_stream_disable_churning, db_.get()));
  ASSERT_EQ(stale_and_active_partitions.stale_partitions.size(), 0);
  ASSERT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  // Verify that both of the active partitions have null parents and children.
  ASSERT_TRUE(stale_and_active_partitions.active_partitions[0].parents.empty());
  ASSERT_TRUE(
      stale_and_active_partitions.active_partitions[0].children.empty());
  ASSERT_TRUE(stale_and_active_partitions.active_partitions[1].parents.empty());
  ASSERT_TRUE(
      stale_and_active_partitions.active_partitions[1].children.empty());
  // One of the initial tokens should have churn type SPLIT. The other initial
  // token should have churn type MOVE.
  if (stale_and_active_partitions.active_partitions[0].next_churn == "MOVE") {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "SPLIT");
  } else {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[0].next_churn,
              "SPLIT");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "MOVE");
  }

  absl::SleepFor(
      absl::GetFlag(FLAGS_change_stream_churn_thread_sleep_interval) * 5);
  ChurnPartitionsForChangeStream(change_stream_disable_churning);

  // One token has split, the other has moved. Thus, there should be 3 active
  // tokens and 2 stale ones.
  bool split_occurred = false;
  bool merge_occurred = false;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      stale_and_active_partitions,
      GetChangeStreamPartitions(change_stream_disable_churning, db_.get()));
  ASSERT_EQ(stale_and_active_partitions.stale_partitions.size(), 2);
  ASSERT_EQ(stale_and_active_partitions.active_partitions.size(), 3);
  for (const auto& active_token :
       stale_and_active_partitions.active_partitions) {
    ASSERT_TRUE(active_token.children.empty());
    ASSERT_FALSE(active_token.parents.empty());
    for (const auto& parent : active_token.parents) {
      if (active_token.parents.size() > 1) {
        merge_occurred = true;
      }
      ZETASQL_ASSERT_OK_AND_ASSIGN(TestChurnedPartitions parent_partition,
                           GetPartition(stale_and_active_partitions, parent));
      // Verify that each parent should contain the token in its children's
      // list.
      ASSERT_THAT(parent_partition.children,
                  testing::Contains(active_token.partition_token));
      ASSERT_EQ(parent_partition.end_time, active_token.start_time);
      if (parent_partition.children.size() > 1) {
        split_occurred = true;
      }
    }
  }
  ASSERT_TRUE(split_occurred);
  ASSERT_FALSE(merge_occurred);
  // One of the tokens has split into two tokens that should be MERGED next
  // time.
  if (stale_and_active_partitions.active_partitions[0].next_churn == "MOVE") {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[2].next_churn,
              "MERGE");
  } else if (stale_and_active_partitions.active_partitions[1].next_churn ==
             "MOVE") {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[0].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[2].next_churn,
              "MERGE");
  } else {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[0].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[2].next_churn,
              "MOVE");
  }

  split_occurred = false;
  merge_occurred = false;
  // Run the churner a second time, verify that one of the tokens has merged.
  absl::SleepFor(
      absl::GetFlag(FLAGS_change_stream_churn_thread_sleep_interval) * 5);
  ChurnPartitionsForChangeStream(change_stream_disable_churning);
  // Two tokens have merged, the other token has moved. Thus, there should be
  // 2+3=5 stale tokens, and two active tokens.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      stale_and_active_partitions,
      GetChangeStreamPartitions(change_stream_disable_churning, db_.get()));
  ASSERT_EQ(stale_and_active_partitions.stale_partitions.size(), 5);
  ASSERT_EQ(stale_and_active_partitions.active_partitions.size(), 2);
  for (const auto& active_token :
       stale_and_active_partitions.active_partitions) {
    ASSERT_TRUE(active_token.children.empty());
    ASSERT_FALSE(active_token.parents.empty());
    if (active_token.parents.size() > 1) {
      merge_occurred = true;
    }
    for (const auto& parent : active_token.parents) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(TestChurnedPartitions parent_partition,
                           GetPartition(stale_and_active_partitions, parent));
      ASSERT_THAT(parent_partition.children,
                  testing::Contains(active_token.partition_token));
      ASSERT_EQ(parent_partition.end_time, active_token.start_time);
      if (parent_partition.children.size() > 1) {
        split_occurred = true;
      }
    }
  }
  ASSERT_TRUE(merge_occurred);
  ASSERT_FALSE(split_occurred);
  // Two tokens have merged into one token that should be SPLIT next time.
  if (stale_and_active_partitions.active_partitions[0].next_churn == "MOVE") {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "SPLIT");
  } else {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[0].next_churn,
              "SPLIT");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "MOVE");
  }

  split_occurred = false;
  merge_occurred = false;
  // Run the churner a third time, verify that one of the tokens has split
  // again.
  absl::SleepFor(
      absl::GetFlag(FLAGS_change_stream_churn_thread_sleep_interval) * 5);
  ChurnPartitionsForChangeStream(change_stream_disable_churning);
  // One token has split, the other has moved. Thus, there should be 5+2=7
  // stale tokens, and three active tokens.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      stale_and_active_partitions,
      GetChangeStreamPartitions(change_stream_disable_churning, db_.get()));
  ASSERT_EQ(stale_and_active_partitions.stale_partitions.size(), 7);
  ASSERT_EQ(stale_and_active_partitions.active_partitions.size(), 3);
  for (const auto& active_token :
       stale_and_active_partitions.active_partitions) {
    ASSERT_TRUE(active_token.children.empty());
    ASSERT_FALSE(active_token.parents.empty());
    if (active_token.parents.size() > 1) {
      merge_occurred = true;
    }
    for (const auto& parent : active_token.parents) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(TestChurnedPartitions parent_partition,
                           GetPartition(stale_and_active_partitions, parent));
      ASSERT_THAT(parent_partition.children,
                  testing::Contains(active_token.partition_token));
      ASSERT_EQ(parent_partition.end_time, active_token.start_time);
      if (parent_partition.children.size() > 1) {
        split_occurred = true;
      }
    }
  }
  // One of the tokens has split into two tokens that should be MERGED next
  // time.
  if (stale_and_active_partitions.active_partitions[0].next_churn == "MOVE") {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[2].next_churn,
              "MERGE");
  } else if (stale_and_active_partitions.active_partitions[1].next_churn ==
             "MOVE") {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[0].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[2].next_churn,
              "MERGE");
  } else {
    ASSERT_EQ(stale_and_active_partitions.active_partitions[0].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[1].next_churn,
              "MERGE");
    ASSERT_EQ(stale_and_active_partitions.active_partitions[2].next_churn,
              "MOVE");
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
