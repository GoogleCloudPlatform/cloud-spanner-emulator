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
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class IndexBackfillTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(CREATE TABLE Users(
          UserId   INT64 NOT NULL,
          Name     STRING(MAX),
          Age      INT64,
        ) PRIMARY KEY (UserId)
      )",
        R"(CREATE TABLE NumericTable(
          key   INT64 NOT NULL,
          val   NUMERIC,
        ) PRIMARY KEY (key)
      )"});
  }
};

TEST_F(IndexBackfillTest, BackfillForUniqueIndexSucceeds) {
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {2, "user1", 25}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {3, "user2", 20}));

  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE UNIQUE INDEX UsersByNameAgeUnique ON Users(Name, Age)"}));

  EXPECT_THAT(ReadWithIndex("Users", "UsersByNameAgeUnique", {"Name", "Age"},
                            KeySet::All()),
              IsOkAndHoldsRows({{"user1", 20}, {"user1", 25}, {"user2", 20}}));
}

TEST_F(IndexBackfillTest,
       BackfillForUniqueIndexFailsWithUniquenessViolationFails) {
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {2, "user", 20}));

  EXPECT_THAT(
      UpdateSchema(
          {"CREATE UNIQUE INDEX UsersByNameAgeUnique ON Users(Name, Age)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(IndexBackfillTest, ValidateCreationOfDuplicateIndexFails) {
  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE INDEX UsersByNameAgeUnique ON Users(Name, Age)"}));
  EXPECT_THAT(
      UpdateSchema({"CREATE INDEX UsersByNameAgeUnique ON Users(Name, Age)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(IndexBackfillTest, BackfillOfIndexOnInterleavedTableSucceeds) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
    CREATE TABLE Photos(
      UserId    INT64 NOT NULL,
      PhotoId   INT64 NOT NULL,
      Name      STRING(MAX),
    ) PRIMARY KEY (UserId, PhotoId),
    INTERLEAVE IN PARENT Users ON DELETE CASCADE
  )"}));

  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("Photos", {"UserId", "PhotoId", "Name"}, {1, 2, "entry1"}));
  ZETASQL_EXPECT_OK(Insert("Photos", {"UserId", "PhotoId", "Name"}, {1, 3, "entry2"}));

  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE INDEX PhotosByName ON Photos(Name, PhotoId, UserId)"}));

  EXPECT_THAT(ReadWithIndex("Photos", "PhotosByName",
                            {"Name", "PhotoId", "UserId"}, KeySet::All()),
              IsOkAndHoldsRows({{"entry1", 2, 1}, {"entry2", 3, 1}}));
}

TEST_F(IndexBackfillTest,
       BackfillOfIndexOnInterleavedTableWithCascadingDeletesSucceeds) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
    CREATE TABLE Photos(
      UserId    INT64 NOT NULL,
      PhotoId   INT64 NOT NULL,
      Name      STRING(MAX),
    ) PRIMARY KEY (UserId, PhotoId),
    INTERLEAVE IN PARENT Users ON DELETE CASCADE
  )"}));

  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("Photos", {"UserId", "PhotoId", "Name"}, {1, 2, "entry1"}));
  ZETASQL_EXPECT_OK(Insert("Photos", {"UserId", "PhotoId", "Name"}, {1, 3, "entry2"}));

  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE INDEX PhotosByName ON Photos(Name, PhotoId)"}));

  ZETASQL_EXPECT_OK(Delete("Users", KeySet::All()));

  EXPECT_THAT(ReadWithIndex("Photos", "PhotosByName", {"Name", "PhotoId"},
                            KeySet::All()),
              IsOkAndHoldsRows({}));
}

TEST_F(IndexBackfillTest, ValidateBackfillOfNullFilteredIndexSucceeds) {
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"UserId", "Name", "Age"}, {2, Null<std::string>(), 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"},
                   {3, "user2", Null<int64_t>()}));

  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE NULL_FILTERED INDEX UsersByNameNullFiltered ON "
                    "Users(Name, Age)"}));

  EXPECT_THAT(ReadWithIndex("Users", "UsersByNameNullFiltered", {"Name", "Age"},
                            KeySet::All()),
              IsOkAndHoldsRows({{"user1", 20}}));
}

TEST_F(IndexBackfillTest, ValidateBackfillOfIndexWithStoredColumns) {
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {2, "user2", 30}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"},
                   {3, "user2", Null<int64_t>()}));

  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE INDEX UsersByName ON Users(Name) STORING (Age)"}));

  EXPECT_THAT(
      ReadWithIndex("Users", "UsersByName", {"Name", "Age"}, KeySet::All()),
      IsOkAndHoldsRows(
          {{"user1", 20}, {"user2", 30}, {"user2", Null<int64_t>()}}));
}

TEST_F(IndexBackfillTest, ValidateNullFilteringDoesNotApplyToStoredColumns) {
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, "user1", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {2, "user2", 30}));
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"},
                   {3, "user2", Null<int64_t>()}));

  // Ensure that null filtering does not apply to stored columns.
  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE NULL_FILTERED INDEX UsersByName ON Users(Name) STORING (Age)"}));

  EXPECT_THAT(
      ReadWithIndex("Users", "UsersByName", {"Name", "Age"}, KeySet::All()),
      IsOkAndHoldsRows(
          {{"user1", 20}, {"user2", 30}, {"user2", Null<int64_t>()}}));
}

TEST_F(IndexBackfillTest, CannotBackfillIndexWithLargeKey) {
  std::string long_name(8192, 'a');
  ZETASQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age"}, {1, long_name, 20}));

  EXPECT_THAT(UpdateSchema({"CREATE INDEX UsersByName ON Users(Name)"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(IndexBackfillTest, BackfillForUniqueNumericIndex) {
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  Numeric val1 = cloud::spanner::MakeNumeric("123.456789").value();
  Numeric val2 = cloud::spanner::MakeNumeric("0").value();
  Numeric val3 = cloud::spanner::MakeNumeric("999999999.456789").value();

  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {1, val1}));
  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {2, val2}));
  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {3, val3}));

  ZETASQL_EXPECT_OK(UpdateSchema({"CREATE UNIQUE INDEX Idx ON NumericTable(val)"}));

  EXPECT_THAT(
      ReadWithIndex("NumericTable", "Idx", {"key", "val"}, KeySet::All()),
      IsOkAndHoldsRows({{2, val2}, {1, val1}, {3, val3}}));
}

TEST_F(IndexBackfillTest,
       BackfillForUniqueNumericIndexFailsWithUniquenessViolationFails) {
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  Numeric val = cloud::spanner::MakeNumeric("123.456789").value();
  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {1, val}));
  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {2, val}));

  EXPECT_THAT(UpdateSchema({"CREATE UNIQUE INDEX Idx ON NumericTable(val)"}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Found uniqueness violation")));
}

TEST_F(IndexBackfillTest, BackfillNullFilteredNumericIndex) {
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  Numeric val = cloud::spanner::MakeNumeric("123.456789").value();
  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {1, Null<Numeric>()}));
  ZETASQL_EXPECT_OK(Insert("NumericTable", {"key", "val"}, {2, val}));

  // Ensure that null filtering does not apply to stored columns.
  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE NULL_FILTERED INDEX Idx ON NumericTable(val)"}));

  EXPECT_THAT(
      ReadWithIndex("NumericTable", "Idx", {"key", "val"}, KeySet::All()),
      IsOkAndHoldsRows({{2, val}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
