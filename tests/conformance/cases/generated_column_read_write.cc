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

using testing::ElementsAre;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

class GeneratedColumnTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(CREATE TABLE T(
          K INT64,
          V1 INT64,
          V2 INT64,
          G1 INT64 AS (G2 + 1) STORED,
          G2 INT64 NOT NULL AS (v1 + v2) STORED,
          G3 INT64 AS (G1) STORED,
          V3 INT64,
        ) PRIMARY KEY (K)
      )",
        R"(CREATE TABLE TypeCoercion(
          K_I INT64,
          G_N NUMERIC AS (K_I) STORED,
          G_F FLOAT64 AS (K_I) STORED,
          G_F_N FLOAT64 AS (G_N) STORED,
        ) PRIMARY KEY (K_I)
      )",
        R"(CREATE TABLE Err(
          K INT64,
          V TIMESTAMP AS (TIMESTAMP_SECONDS(K)) STORED,
        ) PRIMARY KEY (K)
      )",
        "CREATE UNIQUE INDEX TByG3 ON T(G3)",
        R"(CREATE TABLE FK(
          K INT64,
          G INT64 AS (K) STORED,
          FOREIGN KEY (G) REFERENCES T(G3),
        ) PRIMARY KEY (K))",
        R"(CREATE TABLE Ts(
          K TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          V INT64,
          G INT64 AS (V) STORED,
        ) PRIMARY KEY (K))"});
  }
};

MATCHER_P(WhenLowercased, matcher, "") {
  return ::testing::ExplainMatchResult(matcher, absl::AsciiStrToLower(arg),
                                       result_listener);
}

TEST_F(GeneratedColumnTest, TypeCoercion) {
  ZETASQL_EXPECT_OK(Insert("TypeCoercion", {"K_I"}, {1}));
  EXPECT_THAT(ReadAll("TypeCoercion", {"K_I", "G_N", "G_F", "G_F_N"}),
              IsOkAndHoldsRows({{1, cloud::spanner::MakeNumeric("1").value(),
                                 (double)1, (double)1}}));
}

TEST_F(GeneratedColumnTest, Error) {
  ZETASQL_EXPECT_OK(Insert("Err", {"K"}, {1}));
  EXPECT_FALSE(Insert("Err", {"K"}, {253402300800}).status().ok());
}

TEST_F(GeneratedColumnTest, AllMutationTypes) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V1", "V2"}, {1, 1, 1}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 3, 2, 3}}));
  ZETASQL_ASSERT_OK(Update("T", {"K", "V1", "V2"}, {1, 2, 2}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 5, 4, 5}}));
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "V1", "V2"}, {1, 3, 3}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 7, 6, 7}}));
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "V1", "V2"}, {2, 4, 4}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 7, 6, 7}, {2, 9, 8, 9}}));
  ZETASQL_ASSERT_OK(Replace("T", {"K", "V1", "V2"}, {1, 5, 5}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 11, 10, 11}, {2, 9, 8, 9}}));
  ZETASQL_ASSERT_OK(Replace("T", {"K", "V1", "V2"}, {2, 6, 6}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 11, 10, 11}, {2, 13, 12, 13}}));
  ZETASQL_ASSERT_OK(Delete("T", {Key(2)}));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 11, 10, 11}}));
}

TEST_F(GeneratedColumnTest, MultipleMutationsPerRow) {
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T", {"K", "V1", "V2"}, 1, 1, 1),
      MakeUpdate("T", {"K", "V1", "V2"}, 1, 1, 2),
      MakeUpdate("T", {"K", "V1", "V2"}, 1, 2, 2),
      MakeInsert("Err", {"K"}, 1),
      MakeDelete("Err", Singleton(1)),
  }));
  EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 5, 4, 5}}));

  absl::Status s = Commit({
                              MakeInsert("T", {"K"}, 2),
                              MakeUpdate("T", {"K", "V1"}, 2, 3),
                              MakeUpdate("T", {"K", "V2"}, 2, 3),
                          })
                       .status();
  if (in_prod_env()) {
    ZETASQL_ASSERT_OK(s);
    EXPECT_THAT(ReadAll("T", {"K", "G1", "G2", "G3"}),
                IsOkAndHoldsRows({{1, 5, 4, 5}, {2, 7, 6, 7}}));
  } else {
    // Unlike production, the emulator eagerly evaluates generated columns for
    // each individual mutation. So this transaction violates the NOT NULL
    // constraint on G2. This is a fidelity gap we accept in favor of simplicity
    // of the emulator framework.
    EXPECT_THAT(
        s,
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            testing::HasSubstr(
                "Cannot specify a null value for column: T.G2 in table: T")));
  }
}

TEST_F(GeneratedColumnTest, Index) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V1", "V2"}, {1, 1, 1}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{1, 3}}));
  ZETASQL_ASSERT_OK(Update("T", {"K", "V1", "V2"}, {1, 2, 2}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{1, 5}}));
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "V1", "V2"}, {1, 3, 3}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{1, 7}}));
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "V1", "V2"}, {2, 4, 4}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{1, 7}, {2, 9}}));
  ZETASQL_ASSERT_OK(Replace("T", {"K", "V1", "V2"}, {1, 5, 5}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{2, 9}, {1, 11}}));
  ZETASQL_ASSERT_OK(Replace("T", {"K", "V1", "V2"}, {2, 6, 6}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{1, 11}, {2, 13}}));
  ZETASQL_ASSERT_OK(Delete("T", {Key(2)}));
  EXPECT_THAT(ReadAllWithIndex("T", "TByG3", {"K", "G3"}),
              IsOkAndHoldsRows({{1, 11}}));
  EXPECT_THAT(Insert("T", {"K", "V1", "V2"}, {2, 5, 5}),
              StatusIs(absl::StatusCode::kAlreadyExists,
                       WhenLowercased(testing::HasSubstr("unique"))));
}

TEST_F(GeneratedColumnTest, DML) {
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT T(K, V1, V2, V3) VALUES (1, 1, 1, 1)"),
       SqlStatement("UPDATE T SET V1 = 2, V2 = 2, V3 = 2 WHERE K = 1")}));
  EXPECT_THAT(Query("SELECT V1, V2, V3, G1, G2, G3 FROM T"),
              IsOkAndHoldsRows({{2, 2, 2, 5, 4, 5}}));
}

TEST_F(GeneratedColumnTest, NotNull) {
  EXPECT_THAT(Insert("T", {"K", "V1"}, {1, 1}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       WhenLowercased(testing::HasSubstr("null"))));
}

TEST_F(GeneratedColumnTest, ForeignKey) {
  EXPECT_THAT(Insert("FK", {"K"}, {3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Foreign key")));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V1", "V2"}, {1, 1, 1}));
  ZETASQL_EXPECT_OK(Insert("FK", {"K"}, {3}));
  EXPECT_THAT(Delete("T", {Key(1)}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Foreign key")));
}

TEST_F(GeneratedColumnTest, CommitTimestamp) {
  ZETASQL_ASSERT_OK(Insert(
      "Ts", {"K", "V"},
      {google::cloud::spanner::MakeTimestamp(absl::FromUnixMicros(0)).value(),
       0}));
  ZETASQL_ASSERT_OK(Insert("Ts", {"K", "V"}, {"spanner.commit_timestamp()", 1}));
  ZETASQL_ASSERT_OK(Update(
      "Ts", {"K", "V"},
      {google::cloud::spanner::MakeTimestamp(absl::FromUnixMicros(0)).value(),
       2}));
  EXPECT_THAT(ReadAll("Ts", {"G"}), IsOkAndHoldsRows({{2}, {1}}));
}

TEST_F(GeneratedColumnTest, NoDirectWrite) {
  constexpr char kCannotWriteToGeneratedColumn[] =
      "Cannot write into generated column `T.G1`.";
  EXPECT_THAT(Insert("T", {"K", "V1", "V2", "G1"}, {1, 1, 1, 3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr(kCannotWriteToGeneratedColumn)));
  EXPECT_THAT(Update("T", {"K", "V1", "V2", "G1"}, {1, 1, 1, 3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr(kCannotWriteToGeneratedColumn)));
  EXPECT_THAT(InsertOrUpdate("T", {"K", "V1", "V2", "G1"}, {1, 1, 1, 3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr(kCannotWriteToGeneratedColumn)));
  EXPECT_THAT(Replace("T", {"K", "V1", "V2", "G1"}, {1, 1, 1, 3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr(kCannotWriteToGeneratedColumn)));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT T(K, V1, V2) VALUES (1, 1, 1)")}));
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT t(k, v1, v2, g1) VALUES (2, 2, 2, 5)")}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr(
                   "Cannot INSERT value on non-writable column: G1")));
  EXPECT_THAT(CommitDml({SqlStatement("UPDATE t SET g1 = 4 WHERE k = 1")}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(
                           "Cannot UPDATE value on non-writable column: G1")));
}

class GeneratedPrimaryKeyReadWriteTest : public DatabaseTest {
 public:
  GeneratedPrimaryKeyReadWriteTest()
      : feature_flags_({.enable_generated_pk = true}) {}
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"sql(CREATE TABLE T1(
          k1 INT64 ,
          k2 INT64 ,
          k3_stored INT64 NOT NULL AS (k2) STORED,
          k4 INT64 NOT NULL,
        ) PRIMARY KEY (k1, k3_stored))sql",
        R"sql(CREATE TABLE T2(
          k1 INT64 NOT NULL,
          k2_stored INT64 AS (k1*2) STORED,
          k3 INT64 NOT NULL,
          k4_stored INT64 AS (k3*3) STORED,
        ) PRIMARY KEY (k2_stored))sql"});
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(GeneratedPrimaryKeyReadWriteTest, InsertMutations) {
  // A few inserts:
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 5),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 6),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 7),
  }));

  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 5}, {1, 2, 2, 6}, {2, 1, 1, 7}}));

  // All columns are present:
  ZETASQL_ASSERT_OK(Insert("T1", {"k1", "k2", "k4"}, {2, 200, 8}));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows(
          {{1, 1, 1, 5}, {1, 2, 2, 6}, {2, 1, 1, 7}, {2, 200, 200, 8}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, CannotInsertInNonWriteableColumn) {
  // A few inserts:
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 5),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 6),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 7),
  }));

  // Cannot write into a non-writable column k3_stored.
  EXPECT_THAT(Commit({MakeInsert("T1", {"k1", "k3_stored", "k4"}, 3, 4, 9)}),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 5}, {1, 2, 2, 6}, {2, 1, 1, 7}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, DuplicatePrimaryKeyInsertFails) {
  // A few inserts:
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 5),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 6),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 7),
  }));

  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 5}, {1, 2, 2, 6}, {2, 1, 1, 7}}));

  // These inserts fail because the primary key already exists:
  EXPECT_THAT(Insert("T1", {"k1", "k2", "k4"}, {1, 2, 9}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  EXPECT_THAT(Insert("T1", {"k1", "k2", "k4"}, {2, 1, 10}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, InsertOrUpdateMutations) {
  // All 3 inserts.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsertOrUpdate("T1", {"k1", "k2", "k4"}, 2, 1, 2),
      MakeInsertOrUpdate("T1", {"k1", "k2", "k4"}, 1, 2, 3),
  }));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 1}, {2, 1, 1, 2}, {1, 2, 2, 3}}));

  // An insert and an update.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("T1", {"k1", "k2", "k4"}, 3, 3, 9),
      MakeInsertOrUpdate("T1", {"k1", "k2", "k4"}, 3, 3, 300),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsUnorderedRows(
                  {{1, 1, 1, 1}, {2, 1, 1, 2}, {1, 2, 2, 3}, {3, 3, 3, 300}}));

  // All columns are present:
  ZETASQL_ASSERT_OK(InsertOrUpdate("T1", {"k1", "k2", "k4"}, {2, 200, 2}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsUnorderedRows({{1, 1, 1, 1},
                                         {2, 1, 1, 2},
                                         {1, 2, 2, 3},
                                         {3, 3, 3, 300},
                                         {2, 200, 200, 2}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, UpdateMutations) {
  // Insert a few rows and update one.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 2),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 3),
      MakeUpdate("T1", {"k1", "k3_stored", "k4"}, 1, 2, 100),
  }));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 1}, {1, 2, 2, 100}, {2, 1, 1, 3}}));

  ZETASQL_ASSERT_OK(Update("T1", {"k1", "k2", "k4"}, {1, 1, 200}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsUnorderedRows(
                  {{1, 1, 1, 200}, {1, 2, 2, 100}, {2, 1, 1, 3}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, UpdateFailsAllPkColumnsNotPresent) {
  // Insert a few rows and update one.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 2),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 3),
      MakeUpdate("T1", {"k1", "k3_stored", "k4"}, 1, 2, 100),
  }));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 1}, {1, 2, 2, 100}, {2, 1, 1, 3}}));

  // This update fails because all PK columns must be present:
  EXPECT_THAT(Update("T1", {"k1", "k4"}, {1, 1000}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, CannotModifyGeneratedKeyColumn) {
  // Insert a few rows and update one.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 2),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 3),
      MakeUpdate("T1", {"k1", "k3_stored", "k4"}, 1, 2, 100),
  }));
  EXPECT_THAT(
      ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
      IsOkAndHoldsUnorderedRows({{1, 1, 1, 1}, {1, 2, 2, 100}, {2, 1, 1, 3}}));

  // Value of k3 can't be modified due to non key dependent column k2 when
  // trying to update Key(k1(1),k3_stored(1)).
  EXPECT_THAT(Update("T1", {"k1", "k2", "k3_stored", "k4"}, {1, 3, 1, 100}),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, DeleteMutations) {
  // Insert a few rows and delete one.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 2),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 3),
      MakeDelete("T1", Singleton(1, 1)),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 2, 2, 2}, {2, 1, 1, 3}}));

  // This delete fails because all PK columns must be present:
  EXPECT_THAT(Delete("T1", Key(1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 2, 2, 2}, {2, 1, 1, 3}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, DeleteMutationsDifferentCommit) {
  // Insert a few rows and delete one.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 2),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 3),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 1}, {1, 2, 2, 2}, {2, 1, 1, 3}}));
  ZETASQL_EXPECT_OK(Commit({
      MakeDelete("T1", Singleton(1, 1)),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 2, 2, 2}, {2, 1, 1, 3}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, ReplaceMutations) {
  // Insert a few rows and replace one.
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 1, 1),
      MakeInsert("T1", {"k1", "k2", "k4"}, 1, 2, 2),
      MakeInsert("T1", {"k1", "k2", "k4"}, 2, 1, 3),
      MakeReplace("T1", {"k1", "k2", "k4"}, 1, 2, 100),
  }));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 1}, {1, 2, 2, 100}, {2, 1, 1, 3}}));

  ZETASQL_ASSERT_OK(Replace("T1", {"k1", "k2", "k4"}, {1, 1, 200}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 200}, {1, 2, 2, 100}, {2, 1, 1, 3}}));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, DmlInsert) {
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT T1(k1,k2,k4) Values (1,1,1),(2,1,2)")}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 1}, {2, 1, 1, 2}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("INSERT T2(k1,k3) Values (1,1)")}));
  EXPECT_THAT(ReadAll("T2", {"k1", "k3", "k2_stored", "k4_stored"}),
              IsOkAndHoldsRows({{1, 1, 2, 3}}));

  // Cannot write into a non-writable column k3.
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT T1(k1,k3_stored,k4) Values(3,3,3)")}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, DmlUpdate) {
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT T1(k1,k2,k4) Values (1,1,1),(2,1,2)")}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 1}, {2, 1, 1, 2}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("UPDATE T1 SET k4=50 WHERE k3_stored=1")}));
  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 50}, {2, 1, 1, 50}}));

  // Cannot update value of a non-writable column k3.
  EXPECT_THAT(
      CommitDml({SqlStatement("UPDATE T1 SET k3_stored=2 WHERE k3_stored=1")}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(GeneratedPrimaryKeyReadWriteTest, DmlInsertDistinctOnlyGpkValues) {
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT T1(k1,k2,k4) Values (1,1,1),(1,2,2)")}));

  EXPECT_THAT(ReadAll("T1", {"k1", "k2", "k3_stored", "k4"}),
              IsOkAndHoldsRows({{1, 1, 1, 1}, {1, 2, 2, 2}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("INSERT T2(k1,k3) Values (1,1),(2,1)")}));
  EXPECT_THAT(ReadAll("T2", {"k1", "k2_stored", "k3", "k4_stored"}),
              IsOkAndHoldsRows({{1, 2, 1, 3}, {2, 4, 1, 3}}));
}

class PGGeneratedColumnTest : public DatabaseTest {
 public:
  PGGeneratedColumnTest()
      : feature_flags_({.enable_postgresql_interface = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGGeneratedColumnTest, AddJsonbGeneratedColumn) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE t (
          k bigint primary key,
          jsonb_val jsonb default to_jsonb('{"a": 1, "b": 2}'::text),
          jsonb_typ text generated always as (jsonb_typeof(jsonb_val)) stored
        )
      )",
  }));

  EXPECT_THAT(GetDatabaseDdl(), IsOkAndHolds(ElementsAre(R"(CREATE TABLE t (
  k bigint NOT NULL,
  jsonb_val jsonb DEFAULT to_jsonb('{"a": 1, "b": 2}'::text),
  jsonb_typ character varying GENERATED ALWAYS AS (jsonb_typeof(jsonb_val)) STORED,
  PRIMARY KEY(k)
))")));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO t(k) VALUES (1)")}));
  EXPECT_THAT(Query("SELECT jsonb_typ FROM t"), IsOkAndHoldsRows({{"string"}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
