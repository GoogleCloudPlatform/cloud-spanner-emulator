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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using testing::HasSubstr;
using googlesql_base::testing::StatusIs;

class InterleavingTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("interleaving.test");
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in delete tests.
    GOOGLESQL_EXPECT_OK(MultiInsert(
        "Users", {"UserId", "Name"},
        {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}}));

    GOOGLESQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));

    GOOGLESQL_EXPECT_OK(MultiInsert("Messages",
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
    GOOGLESQL_EXPECT_OK(MultiInsert(
        "Snoozes", {"UserId", "ThreadId", "SnoozeId", "SnoozeTs"},
        {
            {1, 1, 1, MakeFutureTimestamp(std::chrono::seconds(600))},
            {1, 3, 1, MakeFutureTimestamp(std::chrono::seconds(1200))},
        }));
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectInterleavingTest, InterleavingTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<InterleavingTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(InterleavingTest, CannotInsertChildWithoutParent) {
  EXPECT_THAT(Insert("Threads", {"UserId", "ThreadId"}, {1, 1}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(InterleavingTest, CanInsertChildWithExistingParent) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"UserId", "Name"}, {1, "Douglas Adams"}));

  GOOGLESQL_EXPECT_OK(Insert("Threads", {"UserId", "ThreadId"}, {1, 1}));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows({{1, 1}}));
}

TEST_P(InterleavingTest, CanInsertParentAndChildInSameTransaction) {
  GOOGLESQL_EXPECT_OK(Commit({
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

TEST_P(InterleavingTest, CanPerformCascadingDeletes) {
  PopulateDatabase();

  // Delete a leaf, parent tables are not affected.
  GOOGLESQL_EXPECT_OK(Delete("Messages", Key(2, 2, 1)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{1, "Douglas Adams"},
                                {2, "Suzanne Collins"},
                                {3, "J.R.R. Tolkien"}}));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows(
                  {{1, 1}, {1, 2}, {1, 3}, {1, 4}, {2, 1}, {2, 2}, {3, 1}}));

  // Delete a subtree, children are deleted but parent tables are not affected.
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(2, 1)));

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

TEST_P(InterleavingTest, CascadingDeletesAreIdempotent) {
  PopulateDatabase();

  // Delete all the rows from all the tables starting with key part 1.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));
  EXPECT_THAT(Read("Users", {"UserId", "Name"}, Key(1)), IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{2, 1, false}, {2, 2, true}, {3, 1, false}}));
  EXPECT_THAT(
      ReadAll("Messages", {"UserId", "ThreadId", "MessageId", "Subject"}),
      IsOkAndHoldsRows({{2, 1, 1, "Lunch today?"},
                        {2, 2, 1, "Suzanne Collins will be absent"},
                        {3, 1, 1, "Interview Notification"}}));

  // Trying to re-delete keys with key part 1 is a no-op at any level.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(1, 1)));
  GOOGLESQL_EXPECT_OK(Delete("Messages", Key(1, 1, 1)));
}

TEST_P(InterleavingTest, CanPerformCascadingRangeDeletes) {
  PopulateDatabase();

  // Delete all threads with key part user_id 2.
  GOOGLESQL_EXPECT_OK(Delete("Threads", OpenClosed(Key(1), Key(2))));
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

TEST_P(InterleavingTest, CannotDeleteRowWithNoActionChildren) {
  PopulateDatabaseWithNoActionChildren();

  // Attempt to delete a Thread fails since an ON DELETE NO ACTION child exists
  // in Snoozes table.
  EXPECT_THAT(Delete("Threads", Key(1, 1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting following thread works since there doesn't exist a corresponding
  // row in Snoozes table.
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(1, 2)));

  // Delete the child in the ON DELETE NO ACTION table, deleting the parent row
  // now succeeds.
  GOOGLESQL_EXPECT_OK(Delete("Snoozes", Key(1, 1, 1)));
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(1, 1)));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_P(InterleavingTest, CannotDeleteRowWithNoActionGrandChildren) {
  PopulateDatabaseWithNoActionChildren();

  // Attempt to delete a row in Users fails since an ON DELETE NO ACTION grand
  // child exists in Snoozes table.
  EXPECT_THAT(Delete("Users", Key(1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting following user works since there doesn't exist a corresponding
  // row in Snoozes table.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(2)));

  // Delete all the corresponding grand children in the ON DELETE NO ACTION
  // table, deleting the grand parent row now succeeds.
  GOOGLESQL_EXPECT_OK(Delete("Snoozes", Key(1, 1, 1)));
  GOOGLESQL_EXPECT_OK(Delete("Snoozes", Key(1, 3, 1)));
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{3, "J.R.R. Tolkien"}}));
}

TEST_P(InterleavingTest, CannotDeleteRowWithNoActionChildrenSameTransaction) {
  PopulateDatabaseWithNoActionChildren();

  // Attempt to delete a parent, then delete no-action child does not work.
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
  GOOGLESQL_EXPECT_OK(Commit({
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

TEST_P(InterleavingTest, CannotInsertAndDeleteRowWithNoActionChild) {
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
  GOOGLESQL_EXPECT_OK(Commit({
      MakeInsert("Users", {"UserId"}, 1),
      MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 1),
      MakeInsert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, 1, 1, 1),
      MakeDelete("Snoozes", child_key_set),
      MakeDelete("Threads", parent_key_set),
  }));
}

TEST_P(InterleavingTest, CannotReplaceRowWithNoActionChild) {
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
  GOOGLESQL_EXPECT_OK(Commit({
      MakeDelete("Snoozes", child_key_set),
      MakeReplace("Threads", {"UserId", "ThreadId", "Starred"}, 1, 1, false),
  }));

  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1, 1)),
              IsOkAndHoldsRow({1, 1, false}));
}

TEST_P(InterleavingTest, CanReplaceRowWithDeleteActionChild) {
  PopulateDatabase();
  // Parent & child rows exist.
  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1, 1)),
              IsOkAndHoldsRow({1, 1, true}));
  EXPECT_THAT(Read("Messages", {"UserId", "ThreadId", "MessageId", "Subject"},
                   ClosedClosed(Key(1, 1, 1), Key(1, 1, 2))),
              IsOkAndHoldsRows({{1, 1, 1, "a code review"},
                                {1, 1, 2, "Re: a code review"}}));

  // Replace on a parent triggers cascading deletes to child table.
  GOOGLESQL_EXPECT_OK(
      Replace("Threads", {"UserId", "ThreadId", "Starred"}, {1, 1, false}));

  // Child rows are deleted.
  EXPECT_THAT(Read("Messages", {"UserId", "ThreadId", "MessageId", "Subject"},
                   ClosedClosed(Key(1, 1, 1), Key(1, 1, 2))),
              IsOkAndHoldsRows({}));
}

class InterleavingNonParentTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_interleave_in = true;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    // The hierarchy is Users ->(IN) Threads -->(IN) Messages
    //                                       |->(IN PARENT) Snoozes*.
    return SetSchemaFromFile("interleaving_non_parent.test");
  }

 protected:
  void PopulateDatabaseWithInterleaveInChildren() {
    // Write fixure data to use in delete tests.
    GOOGLESQL_EXPECT_OK(MultiInsert("Users", {"UserId", "Name"},
                          {{1, "Douglas Adams"}, {2, "Suzanne Collins"}}));

    GOOGLESQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 11, true},
                           {1, 12, true},
                           {1, 13, true},
                           {2, 21, false},
                           {2, 22, true},
                           {3, 31, false},
                           {4, 42, false}}));

    GOOGLESQL_EXPECT_OK(MultiInsert("Messages",
                          {"UserId", "ThreadId", "MessageId", "Subject"},
                          {{1, 11, 111, "a code review"},
                           {1, 11, 112, "Re: a code review"},
                           {1, 12, 121, "Congratulations Douglas"},
                           {1, 13, 131, "Reminder to write feedback"},
                           {2, 21, 211, "Lunch today?"},
                           {2, 22, 221, "Suzanne Collins will be absent"},
                           {3, 31, 311, "Interview Notification"},
                           {4, 42, 421, "Meeting this week"},
                           {5, 53, 531, "Meeting next week"}}));
  }

  void PopulateDatabaseWithInParentChildren() {
    PopulateDatabaseWithInterleaveInChildren();
    GOOGLESQL_EXPECT_OK(MultiInsert(
        "Snoozes", {"UserId", "ThreadId", "SnoozeId", "SnoozeTs"},
        {
            {1, 11, 111, MakeFutureTimestamp(std::chrono::seconds(600))},
            {1, 13, 131, MakeFutureTimestamp(std::chrono::seconds(1200))},
            {3, 31, 311, MakeFutureTimestamp(std::chrono::seconds(1300))},
            {4, 42, 421, MakeFutureTimestamp(std::chrono::seconds(1400))},
        }));
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectInterleavingNonParentTest, InterleavingNonParentTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<InterleavingTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(InterleavingNonParentTest, InsertChildWithOrWithoutExistingParent) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"UserId", "Name"}, {1, "Douglas Adams"}));

  GOOGLESQL_EXPECT_OK(Insert("Threads", {"UserId", "ThreadId"}, {1, 11}));
  GOOGLESQL_EXPECT_OK(Insert("Threads", {"UserId", "ThreadId"}, {2, 21}));  // no parent
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows({{1, 11}, {2, 21}}));

  GOOGLESQL_EXPECT_OK(
      Insert("Messages", {"UserId", "ThreadId", "MessageId"}, {1, 11, 111}));
  GOOGLESQL_EXPECT_OK(
      Insert("Messages", {"UserId", "ThreadId", "MessageId"}, {2, 21, 211}));
  GOOGLESQL_EXPECT_OK(Insert("Messages", {"UserId", "ThreadId", "MessageId"},
                   {3, 31, 311}));  // no parent
  EXPECT_THAT(ReadAll("Messages", {"UserId", "ThreadId", "MessageId"}),
              IsOkAndHoldsRows({{1, 11, 111}, {2, 21, 211}, {3, 31, 311}}));

  // Snoozes is an IN PARENT table.
  GOOGLESQL_EXPECT_OK(
      Insert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, {1, 11, 111}));
  GOOGLESQL_EXPECT_OK(
      Insert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, {2, 21, 211}));
  EXPECT_THAT(
      Insert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, {3, 31, 311}),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_THAT(ReadAll("Snoozes", {"UserId", "ThreadId", "SnoozeId"}),
              IsOkAndHoldsRows({{1, 11, 111}, {2, 21, 211}}));
}

TEST_P(InterleavingNonParentTest, CanInsertParentAndChildInSameTransaction) {
  GOOGLESQL_EXPECT_OK(Commit({
      MakeInsert("Users", {"UserId", "Name"}, 1, "Douglas Adams"),
      MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 11),
      MakeInsert("Threads", {"UserId", "ThreadId"}, 2, 21),
  }));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId"}),
              IsOkAndHoldsRows({{1, 11}, {2, 21}}));

  // The child can be inserted before the parent in the same transaction.
  GOOGLESQL_EXPECT_OK(Commit({
      MakeInsert("Threads", {"UserId", "ThreadId"}, 3, 31),
      MakeInsert("Users", {"UserId", "Name"}, 3, "Bill Goods"),
  }));
}

TEST_P(InterleavingNonParentTest, DeletesDoNotAffectParentOrChildren) {
  PopulateDatabaseWithInParentChildren();

  // Delete a leaf, parent tables are not affected.
  GOOGLESQL_EXPECT_OK(Delete("Messages", Key(2, 22, 221)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{1, "Douglas Adams"}, {2, "Suzanne Collins"}}));
  EXPECT_THAT(
      ReadAll("Threads", {"UserId", "ThreadId"}),
      IsOkAndHoldsRows(
          {{1, 11}, {1, 12}, {1, 13}, {2, 21}, {2, 22}, {3, 31}, {4, 42}}));

  // Delete a row in the middle table, children or parent are not affected.
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(2, 21)));

  EXPECT_THAT(ReadAll("Users", {"UserId", "Name"}),
              IsOkAndHoldsRows({{1, "Douglas Adams"}, {2, "Suzanne Collins"}}));
  EXPECT_THAT(
      ReadAll("Threads", {"UserId", "ThreadId"}),
      IsOkAndHoldsRows({{1, 11}, {1, 12}, {1, 13}, {2, 22}, {3, 31}, {4, 42}}));
  EXPECT_THAT(
      ReadAll("Messages", {"UserId", "ThreadId", "MessageId", "Subject"}),
      IsOkAndHoldsRows({{1, 11, 111, "a code review"},
                        {1, 11, 112, "Re: a code review"},
                        {1, 12, 121, "Congratulations Douglas"},
                        {1, 13, 131, "Reminder to write feedback"},
                        {2, 21, 211, "Lunch today?"},
                        {3, 31, 311, "Interview Notification"},
                        {4, 42, 421, "Meeting this week"},
                        {5, 53, 531, "Meeting next week"}}));

  EXPECT_THAT(ReadAll("Snoozes", {"UserId", "ThreadId", "SnoozeId"}),
              IsOkAndHoldsRows(
                  {{1, 11, 111}, {1, 13, 131}, {3, 31, 311}, {4, 42, 421}}));
}

TEST_P(InterleavingNonParentTest, CannotDeleteRowWithNoActionChildren) {
  PopulateDatabaseWithInParentChildren();

  // Attempt to delete a Thread fails since an ON DELETE NO ACTION child exists
  // in SnoozesNoAction table.
  EXPECT_THAT(Delete("Threads", Key(1, 11)),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting following thread works since there doesn't exist a corresponding
  // row in Snoozes table.
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(1, 12)));

  // Deleting a row in the top level table works.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));

  // Delete the child in the ON DELETE NO ACTION table, deleting the parent row
  // now succeeds.
  GOOGLESQL_EXPECT_OK(Delete("Snoozes", Key(1, 11, 111)));
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(1, 11)));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 13, true},
                                {2, 21, false},
                                {2, 22, true},
                                {3, 31, false},
                                {4, 42, false}}));
}

TEST_P(InterleavingNonParentTest,
       CannotDeleteRowWithNoActionChildrenSameTransaction) {
  PopulateDatabaseWithInParentChildren();

  // Attempt to delete a parent, then delete no-action child does not work.
  KeySet parent_key_set;
  parent_key_set.AddKey(Key(1, 11));
  KeySet child_key_set;
  child_key_set.AddKey(Key(1, 11, 111));
  EXPECT_THAT(Commit({
                  MakeDelete("Threads", parent_key_set),
                  MakeDelete("Snoozes", child_key_set),
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting the no-action child first works.
  GOOGLESQL_EXPECT_OK(Commit({
      MakeDelete("Snoozes", child_key_set),
      MakeDelete("Threads", parent_key_set),
  }));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 12, true},
                                {1, 13, true},
                                {2, 21, false},
                                {2, 22, true},
                                {3, 31, false},
                                {4, 42, false}}));
}

TEST_P(InterleavingNonParentTest,
       DeleteTopLevelRowWithInterleaveInChildrenAndInParentGrandChildren) {
  KeySet parent_key_set;
  parent_key_set.AddKey(Key(1, 11));
  KeySet child_key_set;
  child_key_set.AddKey(Key(1, 11, 111));
  KeySet top_level_key_set;
  top_level_key_set.AddKey(Key(1));

  // Insert a hierarchy with no-action child. Deleting a parent with the
  // inserted no-action child in the same transaction is not allowed.
  EXPECT_THAT(
      Commit({
          MakeInsert("Users", {"UserId"}, 1),
          MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 11),
          MakeInsert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, 1, 11, 111),
          MakeDelete("Threads", parent_key_set),
      }),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Deleting the no-action child first works.
  GOOGLESQL_EXPECT_OK(Commit({
      MakeInsert("Users", {"UserId"}, 1),
      MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 11),
      MakeInsert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, 1, 11, 111),
      MakeDelete("Snoozes", child_key_set),
      MakeDelete("Threads", parent_key_set),
  }));

  // Deleting the top level row works.
  GOOGLESQL_EXPECT_OK(Commit({
      MakeInsert("Threads", {"UserId", "ThreadId"}, 1, 12),
      MakeInsert("Snoozes", {"UserId", "ThreadId", "SnoozeId"}, 1, 12, 121),
      MakeDelete("Users", top_level_key_set),
  }));
}

TEST_P(InterleavingNonParentTest, CannotMigrateToInParentWithMissingParentRow) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL dialect does not allow NULLABLE primary keys";
  }

  GOOGLESQL_EXPECT_OK(Insert("Threads", {"UserId"}, {1}));  // ThreadId is NULL
  GOOGLESQL_EXPECT_OK(Insert("Messages", {"UserId", "MessageId"}, {1, 1}));
  GOOGLESQL_EXPECT_OK(Insert("Messages", {"UserId", "ThreadId", "MessageId"}, {2, 2, 2}));

  EXPECT_THAT(UpdateSchema({
                  R"(
          ALTER TABLE Messages SET INTERLEAVE IN PARENT Threads
        )",
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Parent-child row existence constraint "
                                 "validation failed")));
}

TEST_P(InterleavingNonParentTest,
       CanDeleteAndReplaceRowWithDeleteCascadeGrandChild) {
  GOOGLESQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER TABLE Snoozes SET INTERLEAVE IN PARENT Threads ON DELETE CASCADE
        )",
  }));

  GOOGLESQL_EXPECT_OK(Insert("Users", {"UserId"}, 1));

  GOOGLESQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                        {{1, 11, true}, {3, 31, true}, {4, 41, true}}));

  GOOGLESQL_EXPECT_OK(MultiInsert(
      "Snoozes", {"UserId", "ThreadId", "SnoozeId", "SnoozeTs"},
      {
          {1, 11, 111, MakeFutureTimestamp(std::chrono::seconds(600))},
          {3, 31, 311, MakeFutureTimestamp(std::chrono::seconds(1300))},
          {4, 41, 411, MakeFutureTimestamp(std::chrono::seconds(1500))},
      }));

  // Parent & child rows exist.
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 11, true}, {3, 31, true}, {4, 41, true}}));

  EXPECT_THAT(ReadAll("Snoozes", {"UserId", "ThreadId", "SnoozeId"}),
              IsOkAndHoldsRows({{1, 11, 111}, {3, 31, 311}, {4, 41, 411}}));

  // Replace on the top level row doesn't affect the child nor the grand child.
  GOOGLESQL_EXPECT_OK(Replace("Users",
                    {
                        "UserId",
                        "Name",
                    },
                    {1, "Billy Adams"}));
  // Delete on the top level row doesn't affect the child nor the grand child.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));

  // Verify that all rows are not affected.
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 11, true}, {3, 31, true}, {4, 41, true}}));

  EXPECT_THAT(ReadAll("Snoozes", {"UserId", "ThreadId", "SnoozeId"}),
              IsOkAndHoldsRows({{1, 11, 111}, {3, 31, 311}, {4, 41, 411}}));

  // Replace on a parent triggers cascading delete to child table.
  GOOGLESQL_EXPECT_OK(
      Replace("Threads", {"UserId", "ThreadId", "Starred"}, {1, 11, false}));
  EXPECT_THAT(ReadAll("Snoozes", {"UserId", "ThreadId", "SnoozeId"}),
              IsOkAndHoldsRows({{3, 31, 311}, {4, 41, 411}}));

  // Delete on the parent triggers cascading deletes to grand child table.
  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(3, 31)));
  EXPECT_THAT(ReadAll("Snoozes", {"UserId", "ThreadId", "SnoozeId"}),
              IsOkAndHoldsRows({{4, 41, 411}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
