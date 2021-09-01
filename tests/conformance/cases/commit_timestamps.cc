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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

// String used to tell cloud spanner to insert the commit timestamp into a
// TIMESTAMP column with allow_commit_timestamp option set to true upon
// transaction commit.
constexpr char kCommitTimestampSentinel[] = "spanner.commit_timestamp()";

class CommitTimestamps : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            ID           INT64,
            Name         STRING(MAX),
            Age          INT64,
            CommitTS     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
            NonCommitTS  TIMESTAMP,
          ) PRIMARY KEY (ID)
        )",
        R"(
          CREATE TABLE CommitTimestampKeyTable(
            ID           INT64,
            CommitTS     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
            Name         STRING(MAX),
          ) PRIMARY KEY (ID, CommitTS)
        )",
        R"(
          CREATE TABLE CommitTimestampDescKeyTable(
            ID           INT64,
            CommitTS     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
            Name         STRING(MAX),
          ) PRIMARY KEY (ID, CommitTS DESC)
        )"}));
    return absl::OkStatus();
  }
};

TEST_F(CommitTimestamps, CanWriteCommitTimestampToCommitTimestampColumn) {
  // Test that value assigned to commit timestamp written in column matches the
  // commit timestamp returned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(CommitResult result,
                       Insert("Users", {"ID", "Name", "Age", "CommitTS"},
                              {6, "Levin", 24, kCommitTimestampSentinel}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age", "CommitTS"}),
              IsOkAndHoldsRows({{6, "Levin", 24, result.commit_timestamp}}));
}

TEST_F(CommitTimestamps, CannotWriteCommitTimestampToNonCommitTimestampColumn) {
  // Test that commit timestamp sentinel cannot be passed to a non-timestamp
  // column or to a timestamp column with allow_commit_timestamp set to false.
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"},
                     {6, "Levin", kCommitTimestampSentinel}),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age", "NonCommitTS"},
                     {6, "Levin", 24, kCommitTimestampSentinel}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(CommitTimestamps, CanWriteMaxTimestampToTimestampColumn) {
  // Test that max timestamp value can be written to a non-commit timestamp
  // column.
  Timestamp max_allowed_timestamp = MakeMaxTimestamp();

  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age", "NonCommitTS"},
                   {6, "Levin", 24, max_allowed_timestamp}));

  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age", "NonCommitTS"}),
              IsOkAndHoldsRows({{6, "Levin", 24, max_allowed_timestamp}}));
}

TEST_F(CommitTimestamps, CannotWriteMaxTimestampToCommitTimestampColumn) {
  // Test that max timestamp value cannot be written to a commit timestamp
  // column.
  Timestamp max_allowed_timestamp = MakeMaxTimestamp();

  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age", "CommitTS"},
                     {6, "Levin", 24, max_allowed_timestamp}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(CommitTimestamps, CannotWriteFutureTimestampToCommitTimestampColumn) {
  // Test that future timestamp value cannot be written to a commit timestamp
  // column.
  Timestamp future_timestamp =
      google::cloud::spanner::MakeTimestamp(std::chrono::system_clock::now() +
                                            std::chrono::minutes(1))
          .value();
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age", "CommitTS"},
                     {6, "Levin", 24, future_timestamp}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(CommitTimestamps, CanWriteCommitTimestampToCommitTimestampKeyColumn) {
  // Test that value assigned to commit timestamp written in key column matches
  // the commit timestamp returned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Insert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"},
             {6, kCommitTimestampSentinel, "Mark"}));

  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({{6, result.commit_timestamp, "Mark"}}));
}

TEST_F(CommitTimestamps, CanUpdateCommitTimestampForCommitTimestampColumn) {
  // Test that we can write commit timestamp with Update and InsertOrUpdate.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("Users", {"ID", "Name", "Age", "CommitTS"}, 6, "Levin", 24,
                     kCommitTimestampSentinel),
          MakeUpdate("Users", {"ID", "Name", "Age", "CommitTS"}, 6, "Joseph",
                     25, kCommitTimestampSentinel),
          MakeInsertOrUpdate("Users", {"ID", "Name", "Age", "CommitTS"}, 7,
                             "Mark", 23, kCommitTimestampSentinel),
      }));

  // Check that both the update and insertOrUpdate wrote the same value as the
  // commit timestamp of the transaction.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age", "CommitTS"}),
              IsOkAndHoldsRows({{6, "Joseph", 25, result.commit_timestamp},
                                {7, "Mark", 23, result.commit_timestamp}}));
}

TEST_F(CommitTimestamps, CanUpdateCommitTimestampForCommitTimestampKeyColumn) {
  // Test that we can write commit timestamp with Update and InsertOrUpdate to
  // a key column.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 6,
                     kCommitTimestampSentinel, "Levin"),
          MakeUpdate("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 6,
                     kCommitTimestampSentinel, "Joseph"),
          MakeInsertOrUpdate("CommitTimestampKeyTable",
                             {"ID", "CommitTS", "Name"}, 7,
                             kCommitTimestampSentinel, "Mark"),
      }));

  // Check that both the update and insertOrUpdate wrote the same value to key
  // column as the commit timestamp of the transaction.
  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({{6, result.commit_timestamp, "Joseph"},
                                {7, result.commit_timestamp, "Mark"}}));
}

TEST_F(CommitTimestamps, CanReplaceCommitTimestampForCommitTimestampColumn) {
  // Test that we can write commit timestamp with Replace.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("Users", {"ID", "Name", "Age", "CommitTS"}, 6, "Levin", 24,
                     kCommitTimestampSentinel),
          MakeReplace("Users", {"ID", "Name", "Age", "CommitTS"}, 6, "Mark", 25,
                      kCommitTimestampSentinel),
      }));

  // Check that replace wrote the same value as the commit timestamp of the
  // transaction.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age", "CommitTS"}),
              IsOkAndHoldsRows({{6, "Mark", 25, result.commit_timestamp}}));
}

TEST_F(CommitTimestamps, CanReplaceCommitTimestampForCommitTimestampKeyColumn) {
  // Test that we can write commit timestamp with Replace to a key column.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 6,
                     kCommitTimestampSentinel, "Levin"),
          MakeReplace("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 6,
                      kCommitTimestampSentinel, "Mark"),
      }));

  // Check that replace wrote the same value to key column as the commit
  // timestamp of the transaction.
  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({{6, result.commit_timestamp, "Mark"}}));
}

TEST_F(CommitTimestamps, CanDeleteCommitTimestampColumn) {
  // Insert and delete a row with a commit timestamp column in the same
  // mutation. Read with the same key as deleted should return empty response.
  KeySet key_set;
  key_set.AddKey(Key(6));
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("Users", {"ID", "Name", "Age", "CommitTS"}, 6, "Levin", 24,
                 kCommitTimestampSentinel),
      MakeDelete("Users", key_set),
  }));

  EXPECT_THAT(Read("Users", {"ID", "Name", "Age", "CommitTS"}, Key(6)),
              IsOkAndHoldsRows({}));
}

TEST_F(CommitTimestamps, CanDeleteCommitTimestampKeyColumn) {
  // Insert and delete a row with a commit timestamp key in the same mutation.
  // Read with the commit timestamp key should return empty response.
  KeySet key_set;
  key_set.AddKey(Key(1, kCommitTimestampSentinel));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Levin"),
          MakeDelete("CommitTimestampKeyTable", key_set),
      }));

  EXPECT_THAT(Read("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"},
                   Key(1, result.commit_timestamp)),
              IsOkAndHoldsRows({}));
}

TEST_F(CommitTimestamps, CanDeleteRangeWithCommitTimestampSentinel) {
  // Can use commit timestamp sentinel as part of key bounds for delete ranges.
  // Treated as infinite future when used for end open key bound.
  KeySet key_set;
  key_set.AddKey(Key(1, kCommitTimestampSentinel));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Levin"),
      }));
  EXPECT_THAT(Read("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"},
                   Key(1, result.commit_timestamp)),
              IsOkAndHoldsRows({{1, result.commit_timestamp, "Levin"}}));

  ZETASQL_ASSERT_OK(Delete("CommitTimestampKeyTable",
                   ClosedOpen(Key(1, result.commit_timestamp),
                              Key(1, kCommitTimestampSentinel))));
  EXPECT_THAT(Read("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"},
                   Key(1, result.commit_timestamp)),
              IsOkAndHoldsRows({}));
}

TEST_F(CommitTimestamps, CanDeleteFullRangeInSameTransaction) {
  // Deleting full range in same mutation as commit timestamp inserts will
  // return not found on reads.
  KeySet key_set;
  key_set.AddKey(Key(1, kCommitTimestampSentinel));
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                 kCommitTimestampSentinel, "Levin"),
      MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                 MakeMinTimestamp(), "Mark"),
      MakeDelete("CommitTimestampKeyTable", KeySet::All()),
  }));

  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({}));
}

TEST_F(CommitTimestamps, CannotDeleteEmptyRangeWithCommitTimestampSentinel) {
  // Insert a new record at commit timestamp.
  KeySet key_set;
  key_set.AddKey(Key(1, kCommitTimestampSentinel));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Levin"),
      }));
  EXPECT_THAT(Read("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"},
                   Key(1, result.commit_timestamp)),
              IsOkAndHoldsRows({{1, result.commit_timestamp, "Levin"}}));

  // Performing delete with empty key range with commit timestamp is a no-op.
  ZETASQL_ASSERT_OK(Delete("CommitTimestampKeyTable",
                   ClosedOpen(Key(1, kCommitTimestampSentinel),
                              Key(1, kCommitTimestampSentinel))));
  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({{1, result.commit_timestamp, "Levin"}}));

  // Delete empty range [ct, t0) has no effect (on an insert at t0).
  ZETASQL_ASSERT_OK(Delete("CommitTimestampKeyTable",
                   ClosedOpen(Key(1, kCommitTimestampSentinel),
                              Key(1, result.commit_timestamp))));
  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({{1, result.commit_timestamp, "Levin"}}));
}

TEST_F(CommitTimestamps, ValidatesDeleteRangeWithCommitTimestamp) {
  // NOTE: The emulator differs from test_env in this case on the error code
  // that is produced. The emulator does not differentiate between duplicate
  // values within the same transaction and between transactions unlike prod.
  // Base case, two inserts with same key with CommitTimestamp should fail.
  EXPECT_THAT(
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Levin"),
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Levin"),
      }),
      in_prod_env() ? StatusIs(absl::StatusCode::kInvalidArgument)
                    : StatusIs(absl::StatusCode::kAlreadyExists));

  Timestamp past_timestamp = MakePastTimestamp(std::chrono::seconds(2000));
  Timestamp past_timestamp2 = MakePastTimestamp(std::chrono::seconds(1000));
  Timestamp future_timestamp = MakeFutureTimestamp(std::chrono::seconds(1000));
  Timestamp future_timestamp2 = MakeFutureTimestamp(std::chrono::seconds(2000));

  // Range delete at (past, past+k) is allowed.
  ZETASQL_EXPECT_OK(Delete("CommitTimestampKeyTable",
                   OpenOpen(Key(1, past_timestamp), Key(1, past_timestamp2))));

  // Range delete at (future, future+k) is disallowed.
  EXPECT_THAT(
      Delete("CommitTimestampKeyTable",
             OpenOpen(Key(1, future_timestamp), Key(1, future_timestamp2))),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Range delete at (past, future) disallowed.
  EXPECT_THAT(
      Delete("CommitTimestampKeyTable",
             OpenOpen(Key(1, past_timestamp), Key(1, future_timestamp))),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // No-op delete at (future, future) allowed.
  ZETASQL_EXPECT_OK(
      Delete("CommitTimestampKeyTable",
             OpenOpen(Key(1, future_timestamp), Key(1, future_timestamp))));

  // No-op delete at [future, future) allowed.
  ZETASQL_EXPECT_OK(
      Delete("CommitTimestampKeyTable",
             ClosedOpen(Key(1, future_timestamp), Key(1, future_timestamp))));

  // Degenerate interval [future, future] disallowed (includes future which
  // cannot be specified as commit timestamp value).
  EXPECT_THAT(
      Delete("CommitTimestampKeyTable",
             ClosedClosed(Key(1, future_timestamp), Key(1, future_timestamp))),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // No-op delete at (future, past) is allowed.
  ZETASQL_EXPECT_OK(Delete("CommitTimestampKeyTable",
                   OpenOpen(Key(1, future_timestamp), Key(1, past_timestamp))));

  // Range delete on prefix works.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Levin"),
          MakeDelete("CommitTimestampKeyTable",
                     ClosedClosed(Key(1, MakeNowTimestamp()), Key(1))),
          MakeInsert("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}, 1,
                     kCommitTimestampSentinel, "Mark"),
      }));
  EXPECT_THAT(ReadAll("CommitTimestampKeyTable", {"ID", "CommitTS", "Name"}),
              IsOkAndHoldsRows({{1, result.commit_timestamp, "Mark"}}));
}

TEST_F(CommitTimestamps, ValidatesDeleteRangeWithDescendingCommitTimestamp) {
  Timestamp past_timestamp = MakePastTimestamp(std::chrono::seconds(2000));
  Timestamp past_timestamp2 = MakePastTimestamp(std::chrono::seconds(1000));
  Timestamp future_timestamp = MakeFutureTimestamp(std::chrono::seconds(1000));
  Timestamp future_timestamp2 = MakeFutureTimestamp(std::chrono::seconds(2000));

  // Range delete at (past+k, past) is allowed.
  ZETASQL_EXPECT_OK(Delete("CommitTimestampDescKeyTable",
                   OpenOpen(Key(1, past_timestamp2), Key(1, past_timestamp))));

  // Range delete at (future+k, future) is disallowed.
  EXPECT_THAT(
      Delete("CommitTimestampDescKeyTable",
             OpenOpen(Key(1, future_timestamp2), Key(1, future_timestamp))),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Range delete at (future, past) disallowed.
  EXPECT_THAT(
      Delete("CommitTimestampDescKeyTable",
             OpenOpen(Key(1, future_timestamp), Key(1, past_timestamp))),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // No-op delete at (future, future) allowed.
  ZETASQL_EXPECT_OK(
      Delete("CommitTimestampDescKeyTable",
             OpenOpen(Key(1, future_timestamp), Key(1, future_timestamp))));

  // No-op delete at [future, future) allowed.
  ZETASQL_EXPECT_OK(
      Delete("CommitTimestampDescKeyTable",
             ClosedOpen(Key(1, future_timestamp), Key(1, future_timestamp))));

  // Degenerate interval [future, future] disallowed (includes future which
  // cannot be specified as commit timestamp value).
  EXPECT_THAT(
      Delete("CommitTimestampDescKeyTable",
             ClosedClosed(Key(1, future_timestamp), Key(1, future_timestamp))),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // No-op delete at (past, future) is allowed.
  ZETASQL_EXPECT_OK(Delete("CommitTimestampDescKeyTable",
                   OpenOpen(Key(1, past_timestamp), Key(1, future_timestamp))));

  // Range delete on prefix works.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Commit({
          MakeInsert("CommitTimestampDescKeyTable", {"ID", "CommitTS", "Name"},
                     1, kCommitTimestampSentinel, "Levin"),
          MakeDelete("CommitTimestampDescKeyTable",
                     ClosedClosed(Key(1), Key(1, MakeNowTimestamp()))),
          MakeInsert("CommitTimestampDescKeyTable", {"ID", "CommitTS", "Name"},
                     1, kCommitTimestampSentinel, "Mark"),
      }));
  EXPECT_THAT(
      ReadAll("CommitTimestampDescKeyTable", {"ID", "CommitTS", "Name"}),
      IsOkAndHoldsRows({{1, result.commit_timestamp, "Mark"}}));
}

TEST_F(CommitTimestamps, CanInsertPendingCommitTimestampInBuffer) {
  ZETASQL_ASSERT_OK(ExecuteDml(
      SqlStatement("INSERT INTO CommitTimestampKeyTable (ID, CommitTS, Name) "
                   "VALUES (@id, PENDING_COMMIT_TIMESTAMP(), @name)",
                   {{"id", Value(6)}, {"name", Value("Mark")}})));

  // A query on the table shouldn't see the buffered value since transaction
  // isn't committed yet.
  EXPECT_THAT(Query("SELECT ID, CommitTS, Name FROM CommitTimestampKeyTable"),
              IsOkAndHoldsRows({}));
}

TEST_F(CommitTimestamps, CanInsertAndCommitPendingCommitTimestampInDml) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      CommitDml({SqlStatement(
          "INSERT INTO CommitTimestampKeyTable (ID, CommitTS, Name) "
          "VALUES (@id, PENDING_COMMIT_TIMESTAMP(), @name)",
          {{"id", Value(6)}, {"name", Value("Mark")}})}));
  EXPECT_THAT(Query("SELECT ID, CommitTS, Name FROM CommitTimestampKeyTable"),
              IsOkAndHoldsRows({{6, result.commit_timestamp, "Mark"}}));
}

TEST_F(CommitTimestamps, CanInsertPendingCommitTimestampWithSubsetOfColumns) {
  // Note that column `Name` is not part of inserted columns.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      CommitDml({SqlStatement("INSERT INTO Users(ID, Age, CommitTS) "
                              "VALUES (1, 27, PENDING_COMMIT_TIMESTAMP())")}));
  EXPECT_THAT(Query("SELECT ID, Name, Age, CommitTS FROM Users"),
              IsOkAndHoldsRows(
                  {{1, Null<std::string>(), 27, result.commit_timestamp}}));
}

TEST_F(CommitTimestamps, CanUpdateAndCommitPendingCommitTimestampInDml) {
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "CommitTS"},
                   {1, "Levin", MakeMinTimestamp()}));
  EXPECT_THAT(Query("SELECT ID, Name, CommitTS FROM Users"),
              IsOkAndHoldsRows({{1, "Levin", MakeMinTimestamp()}}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      CommitDml({SqlStatement("UPDATE Users SET CommitTS = "
                              "PENDING_COMMIT_TIMESTAMP() WHERE ID = 1")}));
  EXPECT_THAT(Query("SELECT ID, Name, CommitTS FROM Users"),
              IsOkAndHoldsRows({{1, "Levin", result.commit_timestamp}}));
}

TEST_F(CommitTimestamps, InconsistentCommitTimestampInParent) {
  ZETASQL_EXPECT_OK(SetSchema({
      R"(
        CREATE TABLE T1(
          ts     TIMESTAMP,
        ) PRIMARY KEY (ts)
      )",
      R"(
        CREATE TABLE T2(
          ts     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          col2   STRING(MAX),
        ) PRIMARY KEY (ts, col2), INTERLEAVE IN PARENT T1
      )",
      R"(
        CREATE TABLE T3(
          ts     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          col2   STRING(MAX),
          col3   STRING(MAX),
        ) PRIMARY KEY (ts, col2, col3), INTERLEAVE IN PARENT T2
      )"}));

  // Same key column present in grandparent table's primary key doesn't have
  // commit timestamp enabled.
  EXPECT_THAT(Insert("T3", {"ts", "col2", "col3"},
                     {kCommitTimestampSentinel, "val2", "val3"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(CommitTimestamps, InconsistentCommitTimestampInChildren) {
  ZETASQL_EXPECT_OK(SetSchema({
      R"(
        CREATE TABLE T1(
          ts     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
        ) PRIMARY KEY (ts)
      )",
      R"(
        CREATE TABLE T2(
          ts     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          col2   STRING(MAX),
        ) PRIMARY KEY (ts, col2), INTERLEAVE IN PARENT T1
      )",
      R"(
        CREATE TABLE T3(
          ts     TIMESTAMP,
          col2   STRING(MAX),
          col3   STRING(MAX),
        ) PRIMARY KEY (ts, col2, col3), INTERLEAVE IN PARENT T2
      )"}));

  // Same key column present in grandchild table's primary key doesn't have
  // commit timestamp enabled.
  EXPECT_THAT(Insert("T1", {"ts"}, {kCommitTimestampSentinel}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(CommitTimestamps, InconsistentCommitTimestampAllowedInSchema) {
  ZETASQL_EXPECT_OK(SetSchema({
      R"(
        CREATE TABLE T1(
          ts     TIMESTAMP,
        ) PRIMARY KEY (ts)
      )",
      R"(
        CREATE TABLE T2(
          ts     TIMESTAMP,
          col2   STRING(MAX),
        ) PRIMARY KEY (ts, col2), INTERLEAVE IN PARENT T1
      )"}));

  // Expect that schema change to parent table to set allow_commit_timestamp to
  // true is allowed, even if it's inconsistent with child table. Attempt to
  // insert a pending commit
  ZETASQL_EXPECT_OK(
      SetSchema({"ALTER TABLE T1 ALTER COLUMN ts SET OPTIONS "
                 "(allow_commit_timestamp = true)"}));
  EXPECT_THAT(Insert("T2", {"ts", "col2"}, {kCommitTimestampSentinel, "val2"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Set allow_commit_timestamp to true in child table to make it consistent
  // with the same column in parent table.
  ZETASQL_EXPECT_OK(
      SetSchema({"ALTER TABLE T2 ALTER COLUMN ts SET OPTIONS "
                 "(allow_commit_timestamp = true)"}));

  // Insert in the child table now succeed as expected and the value inserted in
  // the parent and child is same and is equal to the commit timestamp value.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        CommitResult result,
        Commit({
            MakeInsert("T1", {"ts"}, kCommitTimestampSentinel),
            MakeInsert("T2", {"ts", "col2"}, kCommitTimestampSentinel, "val2"),
        }));
    EXPECT_THAT(ReadAll("T1", {"ts"}),
                IsOkAndHoldsRows({{result.commit_timestamp}}));
    EXPECT_THAT(ReadAll("T2", {"ts", "col2"}),
                IsOkAndHoldsRows({{result.commit_timestamp, "val2"}}));
  }

  // Insert a row in parent table first. Followed by an attempt to insert in the
  // child row in a separate commit request.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(CommitResult result,
                         Commit({
                             MakeInsert("T1", {"ts"}, kCommitTimestampSentinel),
                         }));

    // Cannot insert a commit timestamp into child row since it's value will be
    // different from the one inserted in parent.
    EXPECT_THAT(
        Commit({
            MakeInsert("T2", {"ts", "col2"}, kCommitTimestampSentinel, "val2"),
        }),
        StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                               : absl::StatusCode::kNotFound));

    // Insert the same timestamp that was inserted into parent row will succeed.
    ZETASQL_EXPECT_OK(Commit({
        MakeInsert("T2", {"ts", "col2"}, result.commit_timestamp, "val2"),
    }));
  }
}

TEST_F(CommitTimestamps, CanInsertExplicitTimestampWithInconsistentSchema) {
  // Interleave hierarchy with inconsistent allow_commit_timestamp in child.
  ZETASQL_EXPECT_OK(SetSchema({
      R"(
        CREATE TABLE T1(
          ts     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
        ) PRIMARY KEY (ts)
      )",
      R"(
        CREATE TABLE T2(
          ts     TIMESTAMP,
          col2   STRING(MAX),
        ) PRIMARY KEY (ts, col2), INTERLEAVE IN PARENT T1
      )"}));

  // Insert with same explicit timestamp succeeds in inconsistent interleave
  // hierarchy.
  {
    Timestamp explicit_timestamp = MakePastTimestamp(std::chrono::seconds(10));
    ZETASQL_ASSERT_OK(Commit({
        MakeInsert("T1", {"ts"}, explicit_timestamp),
        MakeInsert("T2", {"ts", "col2"}, explicit_timestamp, "val2"),
    }));
    EXPECT_THAT(ReadAll("T1", {"ts"}),
                IsOkAndHoldsRows({{explicit_timestamp}}));
    EXPECT_THAT(ReadAll("T2", {"ts", "col2"}),
                IsOkAndHoldsRows({{explicit_timestamp, "val2"}}));
  }

  // Interleave hierarchy with inconsistent allow_commit_timestamp in parent.
  ZETASQL_EXPECT_OK(SetSchema({
      R"(
        CREATE TABLE T3(
          ts     TIMESTAMP,
        ) PRIMARY KEY (ts)
      )",
      R"(
        CREATE TABLE T4(
          ts     TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          col2   STRING(MAX),
        ) PRIMARY KEY (ts, col2), INTERLEAVE IN PARENT T3
      )"}));

  // Insert with same explicit timestamp succeeds in inconsistent interleave
  // hierarchy.
  {
    Timestamp explicit_timestamp = MakePastTimestamp(std::chrono::seconds(10));
    ZETASQL_ASSERT_OK(Commit({
        MakeInsert("T3", {"ts"}, explicit_timestamp),
        MakeInsert("T4", {"ts", "col2"}, explicit_timestamp, "val2"),
    }));
    EXPECT_THAT(ReadAll("T4", {"ts"}),
                IsOkAndHoldsRows({{explicit_timestamp}}));
    EXPECT_THAT(ReadAll("T4", {"ts", "col2"}),
                IsOkAndHoldsRows({{explicit_timestamp, "val2"}}));
  }
}

// TODO: This is a bug. This actually should pass on the emulator.
// It passes when run against spanner test env.
TEST_F(CommitTimestamps, BatchDmlFailsWithMultipleRowsWithCommitTimestamp) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  auto result = BatchDmlTransaction(
      txn, {SqlStatement("INSERT INTO Users(ID, Name, CommitTS) "
                         "VALUES (1, 'Pete', PENDING_COMMIT_TIMESTAMP())"),
            SqlStatement("INSERT INTO Users(ID, Name, CommitTS) "
                         "VALUES (2, 'Zeke', PENDING_COMMIT_TIMESTAMP())")});
  EXPECT_THAT(ToUtilStatus(result.value().status),
              in_prod_env() ? StatusIs(absl::StatusCode::kOk)
                            : StatusIs(absl::StatusCode::kInvalidArgument));
}

// TODO: This is a bug. Same as above.
TEST_F(CommitTimestamps, BatchDmlFailsWithUpdateWithCommitTimestamp) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  auto result = BatchDmlTransaction(
      txn, {SqlStatement("INSERT INTO Users(ID, Name, CommitTS) "
                         "VALUES (1, 'Pete', PENDING_COMMIT_TIMESTAMP())"),
            SqlStatement("UPDATE Users SET Name = 'Zeke' WHERE ID = 1"),
            SqlStatement("UPDATE Users SET CommitTS = "
                         "PENDING_COMMIT_TIMESTAMP() WHERE ID = 1")});
  EXPECT_THAT(ToUtilStatus(result.value().status),
              in_prod_env() ? StatusIs(absl::StatusCode::kOk)
                            : StatusIs(absl::StatusCode::kInvalidArgument));
}

// Test to verify that commit timestamps are propagated to index columns.
class CommitTimestampIndexingTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Base (
            ID            INT64,
            KeyCommitTS1  TIMESTAMP OPTIONS (allow_commit_timestamp = true),
            KeyCommitTS2  TIMESTAMP OPTIONS (allow_commit_timestamp = true),
            Name          STRING(MAX),
            ValueCommitTS TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          ) PRIMARY KEY (ID, KeyCommitTS1, KeyCommitTS2)
        )",
        R"(
          CREATE INDEX BaseByName
          ON Base(KeyCommitTS1, Name) STORING (ValueCommitTS)
        )"}));
    return absl::OkStatus();
  }
};

TEST_F(CommitTimestampIndexingTest, CommitTimestampIsPropagatedToIndex) {
  // Writing to the base table should write to the index.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      CommitResult result,
      Insert("Base",
             {"ID", "KeyCommitTS1", "KeyCommitTS2", "Name", "ValueCommitTS"},
             {1, kCommitTimestampSentinel, kCommitTimestampSentinel, "Mark",
              kCommitTimestampSentinel}));

  // Read from the base table.
  EXPECT_THAT(
      ReadAll("Base",
              {"ID", "KeyCommitTS1", "KeyCommitTS2", "Name", "ValueCommitTS"}),
      IsOkAndHoldsRows({{1, result.commit_timestamp, result.commit_timestamp,
                         "Mark", result.commit_timestamp}}));

  // Read from the index.
  EXPECT_THAT(
      ReadAllWithIndex(
          "Base", "BaseByName",
          {"ID", "KeyCommitTS1", "KeyCommitTS2", "Name", "ValueCommitTS"}),
      IsOkAndHoldsRows({{1, result.commit_timestamp, result.commit_timestamp,
                         "Mark", result.commit_timestamp}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
