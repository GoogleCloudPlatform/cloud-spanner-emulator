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
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class ColumnDefaultValueReadWriteTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(CREATE TABLE T(
          K INT64,
          D1 INT64 DEFAULT (1),
          G1 INT64 AS (D1 + 1) STORED,
          D2 INT64 NOT NULL DEFAULT (2),
        ) PRIMARY KEY (K))",
        R"(CREATE TABLE T2(
          K INT64,
          D1 INT64,
          D2 INT64 NOT NULL DEFAULT (NULL),
        ) PRIMARY KEY (K))",
        R"(CREATE TABLE TypeCoercion(
          K_I INT64,
          D_N NUMERIC DEFAULT (CAST(1 AS NUMERIC)),
          D_F FLOAT64 DEFAULT (CAST(1 AS FLOAT64)),
        ) PRIMARY KEY (K_I))",
        "CREATE UNIQUE INDEX TByD2 ON T(D2)",
        R"(CREATE TABLE FK(
          K INT64,
          D INT64 DEFAULT (3),
          FOREIGN KEY (D) REFERENCES T(D2),
        ) PRIMARY KEY (K))"});
  }
};

MATCHER_P(WhenLowercased, matcher, "") {
  return ::testing::ExplainMatchResult(matcher, absl::AsciiStrToLower(arg),
                                       result_listener);
}

TEST_F(ColumnDefaultValueReadWriteTest, TypeCoercion) {
  ZETASQL_EXPECT_OK(Insert("TypeCoercion", {"K_I"}, {1}));
  ZETASQL_EXPECT_OK(Insert("TypeCoercion", {"K_I", "D_N", "D_F"},
                   {2, cloud::spanner::MakeNumeric("2").value(), 2.0}));
  EXPECT_THAT(ReadAll("TypeCoercion", {"K_I", "D_N", "D_F"}),
              IsOkAndHoldsRows(
                  {{1, cloud::spanner::MakeNumeric("1").value(), (double)1},
                   {2, cloud::spanner::MakeNumeric("2").value(), (double)2}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, InsertMutations) {
  // All default values
  ZETASQL_ASSERT_OK(Insert("T", {"K"}, {1}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  {
    // This insert fails because the primary key already exist
    EXPECT_THAT(Insert("T", {"K"}, {1}),
                StatusIs(absl::StatusCode::kAlreadyExists));

    // This insert fails because it violate the unique index on D2:
    EXPECT_THAT(Insert("T", {"K", "D2"}, {2, 2}),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }

  // DK has a user value:
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D2"}, {2, 3}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}}));

  // D1 has a user value:
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D1", "D2"}, {3, 11, 4}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}, {3, 11, 12, 4}}));

  // D2 has a user value:
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D2"}, {4, 22}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows(
                  {{1, 1, 2, 2}, {2, 1, 2, 3}, {3, 11, 12, 4}, {4, 1, 2, 22}}));

  // All default columns have user values:
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D1", "D2"}, {5, 2, 5}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2},
                                {2, 1, 2, 3},
                                {3, 11, 12, 4},
                                {4, 1, 2, 22},
                                {5, 2, 3, 5}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, InserOrUpdateMutations) {
  // All default values
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K"}, {1}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K"}, {1}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  // This insert fails because it violate the unique index on D2:
  ASSERT_THAT(InsertOrUpdate("T", {"K", "D2"}, {2, 2}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Insert a second row, D2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D2"}, {2, 3}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}}));

  // Update first row with user values for D2, D1 should remain the same,
  // which is 1, and G1 should be 2:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D2"}, {1, 22}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 1, 2, 3}}));

  // Update second row with user values for D1, D2 should remain the same,
  // which is 3:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D1"}, {2, 11}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}}));

  // Insert a third row, D2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D2"}, {3, 33}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}, {3, 1, 2, 33}}));

  // Insert a fourth row, all default columns have user values:
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D1", "D2"}, {4, 4, 4}));
  EXPECT_THAT(
      ReadAll("T", {"K", "D1", "G1", "D2"}),
      IsOkAndHoldsRows(
          {{1, 1, 2, 22}, {2, 11, 12, 3}, {3, 1, 2, 33}, {4, 4, 5, 4}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, UpdateAndDeleteMutations) {
  // All default values
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K"}, {1}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  // Update a row that doesn't exist
  ASSERT_THAT(Update("T", {"K", "D2"}, {2, 9}),
              StatusIs(absl::StatusCode::kNotFound));

  // Insert a second row, D2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D2"}, {2, 3}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}}));

  // This update fails because it violate the unique index on D2:
  ASSERT_THAT(Update("T", {"K", "D2"}, {2, 2}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Update first row with user values for D2, D1 should remain the same,
  // which is 1, and G1 should be 2:
  ZETASQL_ASSERT_OK(Update("T", {"K", "D2"}, {1, 22}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 1, 2, 3}}));

  // Update second row with user values for D1, D2 should remain the same,
  // which is 3:
  ZETASQL_ASSERT_OK(Update("T", {"K", "D1"}, {2, 11}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}}));

  // Delete a row that doesn't exist is a no-op
  ZETASQL_ASSERT_OK(Delete("T", {Key(3)}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}}));

  // Delete the 2nd row, the first row remains
  ZETASQL_ASSERT_OK(Delete("T", {Key(2)}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, ReplaceMutations) {
  // All user values
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D1", "D2"}, {1, 11, 22}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 11, 12, 22}}));

  // Insert a second row, D2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D2"}, {2, 3}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 11, 12, 22}, {2, 1, 2, 3}}));

  // This replace fails because it violate the unique index on D2:
  ASSERT_THAT(Replace("T", {"K", "D2"}, {2, 22}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Replace first row with user values for D2, D1 and G1 should
  // get computed:
  ZETASQL_ASSERT_OK(Replace("T", {"K", "D2"}, {1, 22}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 1, 2, 3}}));

  // Replace second row with user values for D1, G1 and D2 should
  // get computed:
  ZETASQL_ASSERT_OK(Replace("T", {"K", "D1"}, {2, 111}));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 111, 112, 2}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, MultipleMutationsPerRow) {
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T", {"K", "D1", "D2"}, 1, 1, 1),
      MakeUpdate("T", {"K", "D1", "D2"}, 1, 1, 2),
      MakeUpdate("T", {"K", "D1", "D2"}, 1, 2, 2),
  }));
  EXPECT_THAT(ReadAll("T", {"K", "D1", "G1", "D2"}),
              IsOkAndHoldsRows({{1, 2, 3, 2}}));

  absl::Status s =
      Commit({
                 MakeInsert("T", {"K", "D2"}, 2, 4),
                 MakeUpdate("T", {"K", "D1"}, 2, 3),
                 MakeUpdate("T", {"K", "D2"}, 2, Null<std::int64_t>()),
                 MakeUpdate("T", {"K", "D2"}, 2, 3),
             })
          .status();

  // Users cannot directly providing a NULL value to a NOT NULL column:
  if (in_prod_env()) {
    EXPECT_THAT(
        s, StatusIs(absl::StatusCode::kFailedPrecondition,
                    testing::HasSubstr("The T.D2 column must not be null")));
  } else {
    EXPECT_THAT(
        s,
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            testing::HasSubstr(
                "Cannot specify a null value for column: T.D2 in table: T")));
  }

  // Testing  a NOT NULL DEFAULT NULL column:
  s = Commit({
                 MakeInsert("T2", {"K", "D1"}, 1, 1),
                 MakeUpdate("T2", {"K", "D2"}, 1, 2),
             })
          .status();

  if (in_prod_env()) {
    // In production, users can temporary ignore giving a value to a NOT NULL
    // DEFAULT NULL column, as long as it's fixed later.
    ZETASQL_ASSERT_OK(s);
    EXPECT_THAT(ReadAll("T2", {"K", "D1", "D2"}),
                IsOkAndHoldsRows({{1, 1, 2}}));
  } else {
    // Unlike production, the emulator eagerly evaluates default columns for
    // each individual mutation. So this transaction violates the NOT NULL
    // constraint on D2. This is a fidelity gap we accept in favor of simplicity
    // of the emulator framework.
    EXPECT_THAT(
        s,
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            testing::HasSubstr(
                "Cannot specify a null value for column: T2.D2 in table: T")));
  }
}

TEST_F(ColumnDefaultValueReadWriteTest, Index) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D1", "D2"}, {1, 1, 1}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{1, 1}}));

  ZETASQL_ASSERT_OK(Update("T", {"K", "D1", "D2"}, {1, 2, 2}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{1, 2}}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D1", "D2"}, {1, 3, 3}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{1, 3}}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "D1", "D2"}, {2, 2, 2}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{2, 2}, {1, 3}}));

  ZETASQL_ASSERT_OK(Replace("T", {"K", "D1", "D2"}, {1, 5, 5}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{2, 2}, {1, 5}}));

  ZETASQL_ASSERT_OK(Replace("T", {"K", "D1", "D2"}, {2, 6, 6}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{1, 5}, {2, 6}}));

  ZETASQL_ASSERT_OK(Delete("T", {Key(2)}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByD2", {"K", "D2"}),
              IsOkAndHoldsRows({{1, 5}}));

  EXPECT_THAT(Insert("T", {"K", "D1", "D2"}, {2, 5, 5}),
              StatusIs(absl::StatusCode::kAlreadyExists,
                       WhenLowercased(testing::HasSubstr("unique"))));
}

TEST_F(ColumnDefaultValueReadWriteTest, NotNull) {
  EXPECT_THAT(Insert("T", {"K", "D2"}, {1, Null<std::int64_t>()}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       WhenLowercased(testing::HasSubstr("null"))));
}

TEST_F(ColumnDefaultValueReadWriteTest, ForeignKey) {
  EXPECT_THAT(Insert("FK", {"K"}, {3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Foreign key")));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "D1", "D2"}, {1, 1, 3}));
  ZETASQL_EXPECT_OK(Insert("FK", {"K"}, {3}));
  EXPECT_THAT(Delete("T", {Key(1)}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Foreign key")));
}

TEST_F(ColumnDefaultValueReadWriteTest, DMLsUsingDefaultValues) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT T (K, D2) VALUES (1, 1000), (2, 2000)"),
                 SqlStatement("UPDATE T SET D1 = 100 WHERE K = 2")}));
  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 100, 101, 2000}}));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("UPDATE T SET D1 = DEFAULT WHERE K = 2")}));
  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 1, 2, 2000}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, DMLsInsertWithDefaultKeyword) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      "INSERT T (K, D1, D2) VALUES (3, 300, DEFAULT), (4, DEFAULT, 400)")}));
  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{3, 300, 301, 2}, {4, 1, 2, 400}}));

  // Try delete:
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM T WHERE D1=1")}));
  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{3, 300, 301, 2}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, InvalidUsesOfDefaultKeyword) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT T (K, D2) VALUES (1, 1000), (2, 2000)"),
                 SqlStatement("UPDATE T SET D1 = 100 WHERE K = 2")}));
  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 100, 101, 2000}}));

  ASSERT_THAT(CommitDml({SqlStatement("DELETE FROM T WHERE D1=DEFAULT")}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Unexpected keyword DEFAULT")));

  ASSERT_THAT(CommitDml({SqlStatement("UPDATE T SET D2 = 4 WHERE D1=DEFAULT")}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Unexpected keyword DEFAULT")));

  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 100, 101, 2000}}));
}

TEST_F(ColumnDefaultValueReadWriteTest, DMLsInsertWithUserInput) {
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT T2 (K, D1, D2) VALUES (5, 5, 5), (6, 6, 6)")}));
  EXPECT_THAT(Query("SELECT K, D1, D2 FROM T2 ORDER BY K ASC"),
              IsOkAndHoldsRows({{5, 5, 5}, {6, 6, 6}}));

  // Insert .. select, D1 doesn't appear in the column list so it gets
  // its default value = 1,  G1 is computed from D1
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT T (K, D2) SELECT K,  D2 FROM T2")}));
  EXPECT_THAT(Query("SELECT K, D1, G1, D2 FROM T ORDER BY K ASC"),
              IsOkAndHoldsRows({{5, 1, 2, 5}, {6, 1, 2, 6}}));
}

class DefaultPrimaryKeyReadWriteTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(CREATE TABLE T1(
          k1 INT64,
          k2 INT64 NOT NULL DEFAULT (2),
          a INT64,
        ) PRIMARY KEY (k1, k2))",
        R"(CREATE TABLE T2(
          id1 INT64 DEFAULT (100),
          id2 INT64,
          g1 INT64 AS (id1*2) STORED,
          a INT64,
        ) PRIMARY KEY (id1, id2))"});
  }
};

TEST_F(DefaultPrimaryKeyReadWriteTest, InsertMutations) {
  // A few inserts with default values:
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "a"}, 1, 1),
      MakeInsert("T1", {"a"}, 1),
      MakeInsert("T1", {"k2", "a"}, 1, 1),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1}}));

  // All columns are present:
  ZETASQL_ASSERT_OK(Insert("T1", {"k1", "k2", "a"}, {2, 200, 2}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1},
                                {2, 200, 2}}));

  {
    // These inserts fail because the primary key already exist:
    EXPECT_THAT(Insert("T1", {"k1", "a"}, {1, 2}),
                StatusIs(absl::StatusCode::kAlreadyExists));

    EXPECT_THAT(Insert("T1", {"k2", "a"}, {1, 2}),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }
}

TEST_F(DefaultPrimaryKeyReadWriteTest, InsertOrUpdateMutations) {
  // All 3 inserts
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"a"}, 1),
      MakeInsertOrUpdate("T1", {"k2", "a"}, 1, 1),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1}}));

  // An insert and an update
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "a"}, 3, 3),
      MakeInsertOrUpdate("T1", {"k1", "a"}, 3, 300),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1},
                                {3, 2, 300}}));

  // All columns are present:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T1", {"k1", "k2", "a"}, {2, 200, 2}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1},
                                {2, 200, 2},
                                {3, 2, 300}}));
}

TEST_F(DefaultPrimaryKeyReadWriteTest, UpdateMutations) {
  // Insert a fw rows and update one
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"k2", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"k1", "k2", "a"}, 1, 1, 1),
      MakeUpdate("T1", {"k1", "k2", "a"}, 1, 2, 100),
  }));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 100}}));

  // This update fail because all PK columns must be present:
  EXPECT_THAT(Update("T1", {"k1", "a"}, {1, 1000}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("must have a specific value")));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 100}}));

  ZETASQL_ASSERT_OK(Update("T1", {"k1", "k2", "a"}, {1, 1, 200}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows(
                  {{Null<std::int64_t>(), 1, 1}, {1, 1, 200}, {1, 2, 100}}));
}

TEST_F(DefaultPrimaryKeyReadWriteTest, DeleteMutations) {
  // Insert a few rows and delete one
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"k2", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"k1", "k2", "a"}, 1, 1, 1),
      MakeDelete("T1", Singleton(1, 1)),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 2, 1}}));

  // This delete fails because all PK columns must be present:
  EXPECT_THAT(Delete("T1", Key(1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 2, 1}}));
}

TEST_F(DefaultPrimaryKeyReadWriteTest, ReplaceMutations) {
  // Insert a few rows and replace one
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"k2", "a"}, 1, 1),
      MakeInsertOrUpdate("T1", {"k1", "k2", "a"}, 1, 1, 1),
      MakeReplace("T1", {"k1", "a"}, 1, 100),
  }));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 100}}));

  // The following replace should trigger the default value of k2 to be
  // generated, hence replacing the row (1, 2, 100) with (1, 2, 200).
  // The row (1, 1, 1) should remain the same.
  ZETASQL_ASSERT_OK(Replace("T1", {"k1", "a"}, {1, 200}));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 200}}));
}

TEST_F(DefaultPrimaryKeyReadWriteTest, WithDependentGeneratedColumn) {
  // All 3 inserts
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T2", {"id1", "a"}, 1, 1),
      MakeInsertOrUpdate("T2", {"a"}, 1),
      MakeInsertOrUpdate("T2", {"id2", "a"}, 1, 1),
  }));
  EXPECT_THAT(ReadAll("T2", {"id1", "id2", "g1", "a"}),
              IsOkAndHoldsRows({{1, Null<std::int64_t>(), 2, 1},
                                {100, Null<std::int64_t>(), 200, 1},
                                {100, 1, 200, 1}}));

  // An insert and an update
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T2", {"id2", "a"}, 3, 3),
      MakeInsertOrUpdate("T2", {"id2", "a"}, 3, 300),
  }));
  EXPECT_THAT(ReadAll("T2", {"id1", "id2", "g1", "a"}),
              IsOkAndHoldsRows({{1, Null<std::int64_t>(), 2, 1},
                                {100, Null<std::int64_t>(), 200, 1},
                                {100, 1, 200, 1},
                                {100, 3, 200, 300}}));
}

TEST_F(DefaultPrimaryKeyReadWriteTest, DMLs) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT T2 (id2, a) VALUES (1, 1)")}));
  EXPECT_THAT(Query("SELECT id1, id2, g1, a FROM T2 ORDER BY id2 ASC"),
              IsOkAndHoldsRows({{100, 1, 200, 1}}));

  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT T2 (id1, id2, a) VALUES "
                              "(DEFAULT, 2, 2), (3, 3, 3)")}));
  EXPECT_THAT(
      Query("SELECT id1, id2, g1, a FROM T2 ORDER BY id2 ASC"),
      IsOkAndHoldsRows({{100, 1, 200, 1}, {100, 2, 200, 2}, {3, 3, 6, 3}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
