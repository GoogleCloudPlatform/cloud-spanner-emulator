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

class IndexTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(CREATE TABLE Users(
          ID   INT64 NOT NULL,
          Name STRING(MAX),
          Age  INT64
        ) PRIMARY KEY (ID)
      )",
        "CREATE INDEX UsersByName ON Users(Name)",
        "CREATE INDEX UsersByNameDescending ON Users(Name DESC)",
        "CREATE NULL_FILTERED INDEX UsersByNameNullFiltered ON "
        "Users(Name, Age)",
        "CREATE UNIQUE INDEX UsersByNameAgeUnique ON Users(Name, Age)",
        "CREATE UNIQUE NULL_FILTERED INDEX UsersByNameUniqueFiltered ON "
        "Users(Name)"});
  }
};

TEST_F(IndexTest, ReturnsRowsInDescendingOrder) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {5, Null<std::string>(), 18}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameDescending", {"Name", "ID"}),
      IsOkAndHoldsRows({{"Peter", 2},
                        {"Matthew", 4},
                        {"John", 1},
                        {"Adam", 0},
                        {Null<std::string>(), 5}}));
}

TEST_F(IndexTest, ReturnsRowsInAscendingOrder) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {5, Null<std::string>(), 18}));

  // Read back all rows.
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({{Null<std::string>(), 5},
                                {"Adam", 0},
                                {"John", 1},
                                {"Matthew", 4},
                                {"Peter", 2}}));
}

TEST_F(IndexTest, IndexEntriesAreUpdated) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));
  ZETASQL_EXPECT_OK(Update("Users", {"ID", "Name", "Age"}, {2, "Samantha", 24}));
  ZETASQL_EXPECT_OK(Update("Users", {"ID", "Name", "Age"}, {4, "Alice", 21}));

  // Read back all rows.
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({
                  {"Adam", 0},
                  {"Alice", 4},
                  {"John", 1},
                  {"Samantha", 2},
              }));
}

TEST_F(IndexTest, IndexEntriesAreDeleted) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));

  ZETASQL_EXPECT_OK(Delete("Users", {Key(0), Key(2)}));
  // Read back all rows.
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({{"John", 1}, {"Matthew", 4}}));

  ZETASQL_EXPECT_OK(Delete("Users", {Key(1), Key(4)}));
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({}));
}

TEST_F(IndexTest, EmptyIndexReturnsZeroRows) {
  // Read back all rows.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}), IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({}));
}

TEST_F(IndexTest, NullEntriesAreFiltered) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "", 22}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {2, Null<std::string>(), 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {3, "John", 28}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"},
                   {4, "Matthew", Null<std::int64_t>()}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameNullFiltered",
                       {"Name", "Age", "ID"}),
      IsOkAndHoldsRows({{"", 22, 1}, {"Adam", 20, 0}, {"John", 28, 3}}));
}

TEST_F(IndexTest, AllEntriesAreUnique) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "", 22}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {2, Null<std::string>(), 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {3, "John", 28}));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {4, "Adam", 20}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {5, "", 20}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(
      Insert("Users", {"ID", "Name", "Age"}, {6, Null<std::string>(), 41}),
      StatusIs(absl::StatusCode::kAlreadyExists));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"},
                   {7, "Matthew", Null<std::int64_t>()}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameAgeUnique", {"Name", "Age", "ID"}),
      IsOkAndHoldsRows({{Null<std::string>(), 41, 2},
                        {"", 22, 1},
                        {"Adam", 20, 0},
                        {"John", 28, 3},
                        {"Matthew", Null<std::int64_t>(), 7}}));
}

TEST_F(IndexTest, TriggersUniqueIndexViolationWithImplicitNulls) {
  // In both cases, NULL value trriggers a Unique index violations for primary
  // key "Name, Age" in UsersByNameAgeUnique index.

  // Executed across separate transactions.
  {
    // Index UsersByNameAgeUnique will add NULL, NULL for Name & Age column.
    ZETASQL_EXPECT_OK(Insert("Users", {"ID"}, {0}));

    // This should fail because it is also adding NULL, NULL to unique Index
    // UsersByNameAgeUnique.
    EXPECT_THAT(Insert("Users", {"ID"}, {1}),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }

  // Executed within same transaction.
  {
    auto txn = Transaction(Transaction::ReadWriteOptions());
    EXPECT_THAT(
        CommitTransaction(txn, {MakeInsertOrUpdate("Users", {"ID"}, Value(0)),
                                MakeInsert("Users", {"ID"}, Value(1))}),
        StatusIs(absl::StatusCode::kAlreadyExists));
  }
}

TEST_F(IndexTest, AllEntriesAreUniqueAndNullFiltered) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "", 22}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {2, Null<std::string>(), 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {3, "John", 28}));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {4, "Adam", 20}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {5, "", 22}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  // A duplicate index entry that is null filtered should not trigger a UNIQUE
  // violation.
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {6, Null<std::string>(), 43}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"},
                   {7, "Matthew", Null<std::int64_t>()}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameUniqueFiltered", {"Name", "ID"}),
      IsOkAndHoldsRows({{"", 1}, {"Adam", 0}, {"John", 3}, {"Matthew", 7}}));
}

TEST_F(IndexTest, ValidateKeyTooLargeFails) {
  std::string long_name(8192, 'a');
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {1, long_name, 20}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

class NumericIndexTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchema({
        R"(CREATE TABLE Accounts(
          ID   INT64 NOT NULL,
          Name STRING(MAX),
          Money NUMERIC
        ) PRIMARY KEY (ID)
      )",
        "CREATE INDEX AccountsByNameStoringMoney ON Accounts(Name) STORING "
        "(Money)",
        "CREATE INDEX AccountsByMoney ON Accounts(Money)",
    });
  }
};

TEST_F(NumericIndexTest, BasicRead) {
  Numeric adam_money =
      cloud::spanner::MakeNumeric("-9999999999999999123.456789").value();
  Numeric bill_money = cloud::spanner::MakeNumeric("123.456789").value();
  Numeric john_money = cloud::spanner::MakeNumeric("0").value();
  Numeric zack_money = cloud::spanner::MakeNumeric("999999999.456789").value();

  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {0, "Zack", zack_money}));
  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {1, "John", john_money}));
  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {2, "Adam", adam_money}));
  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {3, "Bill", bill_money}));

  EXPECT_THAT(ReadAllWithIndex("Accounts", "AccountsByNameStoringMoney",
                               {"Name", "Money"}),
              IsOkAndHoldsRows({{"Adam", adam_money},
                                {"Bill", bill_money},
                                {"John", john_money},
                                {"Zack", zack_money}}));

  EXPECT_THAT(ReadAllWithIndex("Accounts", "AccountsByMoney", {"Money"}),
              IsOkAndHoldsRows(
                  {{adam_money}, {john_money}, {bill_money}, {zack_money}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
