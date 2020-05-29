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
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class InterleavingTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(
        CREATE TABLE Users (
          UserId     INT64 NOT NULL,
          Name       STRING(MAX),
        ) PRIMARY KEY (UserId)
      )",
        R"(
        CREATE TABLE Threads (
          UserId     INT64 NOT NULL,
          ThreadId   INT64 NOT NULL,
          Starred    BOOL
        ) PRIMARY KEY (UserId, ThreadId),
        INTERLEAVE IN PARENT Users ON DELETE CASCADE
      )",
        R"(
        CREATE TABLE Messages (
          UserId     INT64 NOT NULL,
          ThreadId   INT64 NOT NULL,
          MessageId  INT64 NOT NULL,
          Subject    STRING(MAX),
        ) PRIMARY KEY (UserId, ThreadId, MessageId),
        INTERLEAVE IN PARENT Threads ON DELETE CASCADE
      )",
        R"(
        CREATE TABLE Snoozes (
          UserId     INT64 NOT NULL,
          ThreadId   INT64 NOT NULL,
          SnoozeId   INT64 NOT NULL,
          SnoozeTs   Timestamp,
        ) PRIMARY KEY (UserId, ThreadId, SnoozeId),
        INTERLEAVE IN PARENT Threads ON DELETE NO ACTION
      )"});
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in delete tests.
    ZETASQL_EXPECT_OK(MultiInsert(
        "Users", {"UserId", "Name"},
        {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}}));

    ZETASQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));

    ZETASQL_EXPECT_OK(MultiInsert("Messages",
                          {"UserId", "ThreadId", "MessageId", "Subject"},
                          {{1, 1, 1, "a code review"},
                           {1, 1, 2, "Re: a code review"},
                           {1, 2, 1, "Congratulations Douglas"},
                           {1, 3, 1, "Reminder to write feedback"},
                           {1, 4, 1, "Meeting this week"},
                           {2, 1, 1, "Lunch today?"},
                           {2, 2, 1, "Suzanne Collins will be absent"},
                           {3, 1, 1, "Interview Notification"}}));
  }

  void PopulateDatabaseWithNoActionChildren() {
    PopulateDatabase();
    ZETASQL_EXPECT_OK(MultiInsert(
        "Snoozes", {"UserId", "ThreadId", "SnoozeId", "SnoozeTs"},
        {
            {1, 1, 1, MakeFutureTimestamp(std::chrono::seconds(600))},
            {1, 3, 1, MakeFutureTimestamp(std::chrono::seconds(1200))},
        }));
  }
};

TEST_F(InterleavingTest, CannotInsertChildWithoutParent) {
  EXPECT_THAT(Insert("Threads", {"UserId", "ThreadId"}, {1, 1}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InterleavingTest, CanInsertChildWithExistingParent) {
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name"}, {1, "Douglas Adams"}));

  ZETASQL_EXPECT_OK(Insert("Threads", {"UserId", "ThreadId"}, {1, 1}));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows({{1, 1}}));
}

TEST_F(InterleavingTest, CanInsertParentAndChildInSameTransaction) {
  ZETASQL_EXPECT_OK(Commit({
      MakeInsert("Users", {"UserId", "Name"}, 1, "Douglas Adams"),
      MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 1),
  }));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows({{1, 1}}));

  // Though child cannot be inserted before the parent in the same transaction.
  EXPECT_THAT(Commit({
                  MakeInsert("Threads", {"UserId", "ThreadId"}, 2, 1),
                  MakeInsert("Users", {"UserId", "Name"}, 2, "Douglas Adams"),
              }),
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound));
}

TEST_F(InterleavingTest, CanPerformCascadingDeletes) {
  PopulateDatabase();

  // Delete a leaf, parent tables are not affected.
  ZETASQL_EXPECT_OK(Delete("Messages", Key(2, 2, 1)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{1, "Douglas Adams"},
                                {2, "Suzanne Collins"},
                                {3, "J.R.R. Tolkien"}}));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows(
                  {{1, 1}, {1, 2}, {1, 3}, {1, 4}, {2, 1}, {2, 2}, {3, 1}}));

  // Delete a subtree, children are deleted but parent tables are not affected.
  ZETASQL_EXPECT_OK(Delete("Threads", Key(2, 1)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{1, "Douglas Adams"},
                                {2, "Suzanne Collins"},
                                {3, "J.R.R. Tolkien"}}));
  EXPECT_THAT(
      ReadAll("Threads", {"UserId", "ThreadId"}),
      IsOkAndHoldsRows({{1, 1}, {1, 2}, {1, 3}, {1, 4}, {2, 2}, {3, 1}}));
  EXPECT_THAT(
      ReadAll("Messages", {"UserId", "ThreadId", "MessageId", "Subject"}),
      IsOkAndHoldsRows({{1, 1, 1, "a code review"},
                        {1, 1, 2, "Re: a code review"},
                        {1, 2, 1, "Congratulations Douglas"},
                        {1, 3, 1, "Reminder to write feedback"},
                        {1, 4, 1, "Meeting this week"},
                        {3, 1, 1, "Interview Notification"}}));
}

TEST_F(InterleavingTest, CascadingDeletesAreIdempotent) {
  PopulateDatabase();

  // Delete all the rows from all the tables starting with key part 1.
  ZETASQL_EXPECT_OK(Delete("Users", Key(1)));
  EXPECT_THAT(Read("Users", {"UserId", "Name"}, Key(1)), IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{2, 1, false}, {2, 2, true}, {3, 1, false}}));
  EXPECT_THAT(
      ReadAll("Messages", {"UserId", "ThreadId", "MessageId", "Subject"}),
      IsOkAndHoldsRows({{2, 1, 1, "Lunch today?"},
                        {2, 2, 1, "Suzanne Collins will be absent"},
                        {3, 1, 1, "Interview Notification"}}));

  // Trying to re-delete keys with key part 1 is a no-op at any level.
  ZETASQL_EXPECT_OK(Delete("Users", Key(1)));
  ZETASQL_EXPECT_OK(Delete("Threads", Key(1, 1)));
  ZETASQL_EXPECT_OK(Delete("Messages", Key(1, 1, 1)));
}

TEST_F(InterleavingTest, CanPerformCascadingRangeDeletes) {
  PopulateDatabase();

  // Delete all threads with key part user_id 2.
  ZETASQL_EXPECT_OK(Delete("Threads", OpenClosed(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));

  // All messages with thread_id 2 as key_part are also deleted.
  EXPECT_THAT(
      ReadAll("Messages", {"UserId", "ThreadId", "MessageId", "Subject"}),
      IsOkAndHoldsRows({{1, 1, 1, "a code review"},
                        {1, 1, 2, "Re: a code review"},
                        {1, 2, 1, "Congratulations Douglas"},
                        {1, 3, 1, "Reminder to write feedback"},
                        {1, 4, 1, "Meeting this week"},
                        {3, 1, 1, "Interview Notification"}}));
}

TEST_F(InterleavingTest, CannotDeleteRowWithNoActionChildren) {
  PopulateDatabaseWithNoActionChildren();

  // Attempt to delete a Thread fails since an ON DELETE NO ACTION child exists
  // in Snoozes table.
  EXPECT_THAT(Delete("Threads", Key(1, 1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting following thread works since there doesn't exist a corresponding
  // row in Snoozes table.
  ZETASQL_EXPECT_OK(Delete("Threads", Key(1, 2)));

  // Delete the child in the ON DELETE NO ACTION table, deleting the parent row
  // now succeeds.
  ZETASQL_EXPECT_OK(Delete("Snoozes", Key(1, 1, 1)));
  ZETASQL_EXPECT_OK(Delete("Threads", Key(1, 1)));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_F(InterleavingTest, CannotDeleteRowWithNoActionGrandChildren) {
  PopulateDatabaseWithNoActionChildren();

  // Attempt to delete a row in Users fails since an ON DELETE NO ACTION grand
  // child exists in Snoozes table.
  EXPECT_THAT(Delete("Users", Key(1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting following user works since there doesn't exist a corresponding
  // row in Snoozes table.
  ZETASQL_EXPECT_OK(Delete("Users", Key(2)));

  // Delete all the corresponding grand children in the ON DELETE NO ACTION
  // table, deleting the grand parent row now succeeds.
  ZETASQL_EXPECT_OK(Delete("Snoozes", Key(1, 1, 1)));
  ZETASQL_EXPECT_OK(Delete("Snoozes", Key(1, 3, 1)));
  ZETASQL_EXPECT_OK(Delete("Users", Key(1)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{3, "J.R.R. Tolkien"}}));
}

TEST_F(InterleavingTest, CannotDeleteRowWithNoActionChildrenSameTransaction) {
  PopulateDatabaseWithNoActionChildren();

  // Attemp to delete a parent, then delete no-action child does not work.
  KeySet parent_key_set;
  parent_key_set.AddKey(Key(1, 1));
  KeySet child_key_set;
  child_key_set.AddKey(Key(1, 1, 1));
  EXPECT_THAT(Commit({
                  MakeDelete("Threads", parent_key_set),
                  MakeDelete("Snoozes", child_key_set),
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting the no-action child first works.
  ZETASQL_EXPECT_OK(Commit({
      MakeDelete("Snoozes", child_key_set),
      MakeDelete("Threads", parent_key_set),
  }));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_F(InterleavingTest, CannotInsertAndDeleteRowWithNoActionChild) {
  KeySet parent_key_set;
  parent_key_set.AddKey(Key(1, 1));
  KeySet child_key_set;
  child_key_set.AddKey(Key(1, 1, 1));

  // Insert a hierarchy with no-action child. Deleting a parent with the
  // inserted no-action child in the same transaction is not allowed.
  EXPECT_THAT(
      Commit({
          MakeInsert("Users", {"UserId"}, 1),
          MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 1),
          MakeInsert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, 1, 1, 1),
          MakeDelete("Threads", parent_key_set),
      }),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting the no-action child first works.
  ZETASQL_EXPECT_OK(Commit({
      MakeInsert("Users", {"UserId"}, 1),
      MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 1),
      MakeInsert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, 1, 1, 1),
      MakeDelete("Snoozes", child_key_set),
      MakeDelete("Threads", parent_key_set),
  }));
}

TEST_F(InterleavingTest, CannotReplaceRowWithNoActionChild) {
  PopulateDatabaseWithNoActionChildren();
  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1, 1)),
              IsOkAndHoldsRow({1, 1, true}));

  // Replace on a parent with no-action child does not work.
  EXPECT_THAT(
      Replace("Threads", {"UserId", "ThreadId", "Starred"}, {1, 1, false}),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Replace does not work even if replacing to same value.
  EXPECT_THAT(
      Replace("Threads", {"UserId", "ThreadId", "Starred"}, {1, 1, true}),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // However, replace works if no-action child is deleted first in the same
  // transaction.
  KeySet child_key_set;
  child_key_set.AddKey(Key(1, 1, 1));
  ZETASQL_EXPECT_OK(Commit({
      MakeDelete("Snoozes", child_key_set),
      MakeReplace("Threads", {"UserId", "ThreadId", "Starred"}, 1, 1, false),
  }));

  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1, 1)),
              IsOkAndHoldsRow({1, 1, false}));
}

TEST_F(InterleavingTest, CanReplaceRowWithDeleteActionChild) {
  PopulateDatabase();
  // Parent & child rows exist.
  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1, 1)),
              IsOkAndHoldsRow({1, 1, true}));
  EXPECT_THAT(Read("Messages", {"UserId", "ThreadId", "MessageId", "Subject"},
                   ClosedClosed(Key(1, 1, 1), Key(1, 1, 2))),
              IsOkAndHoldsRows({{1, 1, 1, "a code review"},
                                {1, 1, 2, "Re: a code review"}}));

  // Replace on a parent triggers cascading deletes to child table.
  ZETASQL_EXPECT_OK(
      Replace("Threads", {"UserId", "ThreadId", "Starred"}, {1, 1, false}));

  // Child rows are deleted.
  EXPECT_THAT(Read("Messages", {"UserId", "ThreadId", "MessageId", "Subject"},
                   ClosedClosed(Key(1, 1, 1), Key(1, 1, 2))),
              IsOkAndHoldsRows({}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
