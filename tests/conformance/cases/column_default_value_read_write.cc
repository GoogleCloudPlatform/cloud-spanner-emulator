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

#include <cstdint>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/numeric.h"
#include "google/cloud/spanner/timestamp.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class ColumnDefaultValueReadWriteTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("column_default_value.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_setter_ =
      test::ScopedEmulatorFeatureFlagsSetter(
          {.enable_dml_returning = true,
           .enable_upsert_queries = true,
           .enable_user_defined_functions = true});
};

MATCHER_P(WhenLowercased, matcher, "") {
  return ::testing::ExplainMatchResult(matcher, absl::AsciiStrToLower(arg),
                                       result_listener);
}

INSTANTIATE_TEST_SUITE_P(
    PerDialectColumnDefaultValueTests, ColumnDefaultValueReadWriteTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ColumnDefaultValueReadWriteTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(ColumnDefaultValueReadWriteTest, TypeCoercion) {
  ZETASQL_EXPECT_OK(Insert("type_coercion", {"k_i"}, {1}));
  ZETASQL_EXPECT_OK(Insert("type_coercion", {"k_i", "d_n", "d_f"},
                   {2, cloud::spanner::MakeNumeric("2").value(), 2.0}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        ReadAll("type_coercion", {"k_i", "d_n", "d_f"}),
        IsOkAndHoldsRows(
            {{1, cloud::spanner::MakePgNumeric("1").value(), (double)1},
             {2, cloud::spanner::MakePgNumeric("2").value(), (double)2}}));
  } else {
    EXPECT_THAT(
        ReadAll("type_coercion", {"k_i", "d_n", "d_f"}),
        IsOkAndHoldsRows(
            {{1, cloud::spanner::MakeNumeric("1").value(), (double)1},
             {2, cloud::spanner::MakeNumeric("2").value(), (double)2}}));
  }
}

TEST_P(ColumnDefaultValueReadWriteTest, InsertMutations) {
  // All default values
  ZETASQL_ASSERT_OK(Insert("t", {"k"}, {1}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  {
    // This insert fails because the primary key already exist
    EXPECT_THAT(Insert("t", {"k"}, {1}),
                StatusIs(absl::StatusCode::kAlreadyExists));

    // This insert fails because it violate the unique index on d2:
    EXPECT_THAT(Insert("t", {"k", "d2"}, {2, 2}),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }

  // DK has a user value:
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d2"}, {2, 3}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}}));

  // d1 has a user value:
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d1", "d2"}, {3, 11, 4}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}, {3, 11, 12, 4}}));

  // d2 has a user value:
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d2"}, {4, 22}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows(
                  {{1, 1, 2, 2}, {2, 1, 2, 3}, {3, 11, 12, 4}, {4, 1, 2, 22}}));

  // All default columns have user values:
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d1", "d2"}, {5, 2, 5}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2},
                                {2, 1, 2, 3},
                                {3, 11, 12, 4},
                                {4, 1, 2, 22},
                                {5, 2, 3, 5}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, InserOrUpdateMutations) {
  // All default values
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k"}, {1}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k"}, {1}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  // This insert fails because it violate the unique index on d2:
  ASSERT_THAT(InsertOrUpdate("t", {"k", "d2"}, {2, 2}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Insert a second row, d2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d2"}, {2, 3}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}}));

  // Update first row with user values for d2, d1 should remain the same,
  // which is 1, and g1 should be 2:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d2"}, {1, 22}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 1, 2, 3}}));

  // Update second row with user values for d1, d2 should remain the same,
  // which is 3:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d1"}, {2, 11}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}}));

  // Insert a third row, d2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d2"}, {3, 33}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}, {3, 1, 2, 33}}));

  // Insert a fourth row, all default columns have user values:
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d1", "d2"}, {4, 4, 4}));
  EXPECT_THAT(
      ReadAll("t", {"k", "d1", "g1", "d2"}),
      IsOkAndHoldsRows(
          {{1, 1, 2, 22}, {2, 11, 12, 3}, {3, 1, 2, 33}, {4, 4, 5, 4}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, UpdateAndDeleteMutations) {
  // All default values
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k"}, {1}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}}));

  // Update a row that doesn't exist
  ASSERT_THAT(Update("t", {"k", "d2"}, {2, 9}),
              StatusIs(absl::StatusCode::kNotFound));

  // Insert a second row, d2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d2"}, {2, 3}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 2}, {2, 1, 2, 3}}));

  // This update fails because it violate the unique index on d2:
  ASSERT_THAT(Update("t", {"k", "d2"}, {2, 2}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Update first row with user values for d2, d1 should remain the same,
  // which is 1, and g1 should be 2:
  ZETASQL_ASSERT_OK(Update("t", {"k", "d2"}, {1, 22}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 1, 2, 3}}));

  // Update second row with user values for d1, d2 should remain the same,
  // which is 3:
  ZETASQL_ASSERT_OK(Update("t", {"k", "d1"}, {2, 11}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}}));

  // Delete a row that doesn't exist is a no-op
  ZETASQL_ASSERT_OK(Delete("t", {Key(3)}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 11, 12, 3}}));

  // Delete the 2nd row, the first row remains
  ZETASQL_ASSERT_OK(Delete("t", {Key(2)}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, ReplaceMutations) {
  // All user values
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d1", "d2"}, {1, 11, 22}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 11, 12, 22}}));

  // Insert a second row, d2 has a user value:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d2"}, {2, 3}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 11, 12, 22}, {2, 1, 2, 3}}));

  // This replace fails because it violate the unique index on d2:
  ASSERT_THAT(Replace("t", {"k", "d2"}, {2, 22}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Replace first row with user values for d2, d1 and g1 should
  // get computed:
  ZETASQL_ASSERT_OK(Replace("t", {"k", "d2"}, {1, 22}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 1, 2, 3}}));

  // Replace second row with user values for d1, g1 and d2 should
  // get computed:
  ZETASQL_ASSERT_OK(Replace("t", {"k", "d1"}, {2, 111}));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 1, 2, 22}, {2, 111, 112, 2}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, MultipleMutationsPerRow) {
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("t", {"k", "d1", "d2"}, 1, 1, 1),
      MakeUpdate("t", {"k", "d1", "d2"}, 1, 1, 2),
      MakeUpdate("t", {"k", "d1", "d2"}, 1, 2, 2),
  }));
  EXPECT_THAT(ReadAll("t", {"k", "d1", "g1", "d2"}),
              IsOkAndHoldsRows({{1, 2, 3, 2}}));

  absl::Status s =
      Commit({
                 MakeInsert("t", {"k", "d2"}, 2, 4),
                 MakeUpdate("t", {"k", "d1"}, 2, 3),
                 MakeUpdate("t", {"k", "d2"}, 2, Null<std::int64_t>()),
                 MakeUpdate("t", {"k", "d2"}, 2, 3),
             })
          .status();

  // Users cannot directly providing a NULL value to a NOT NULL column:
  if (in_prod_env()) {
    EXPECT_THAT(
        s, StatusIs(absl::StatusCode::kFailedPrecondition,
                    testing::HasSubstr("The t.d2 column must not be null")));
  } else {
    EXPECT_THAT(
        s,
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            testing::HasSubstr(
                "Cannot specify a null value for column: t.d2 in table: t")));
  }

  // Testing  a NOT NULL DEFAULT NULL column:
  s = Commit({
                 MakeInsert("t2", {"k", "d1"}, 1, 1),
                 MakeUpdate("t2", {"k", "d2"}, 1, 2),
             })
          .status();

  if (in_prod_env()) {
    // In production, users can temporary ignore giving a value to a NOT NULL
    // DEFAULT NULL column, as long as it's fixed later.
    ZETASQL_ASSERT_OK(s);
    EXPECT_THAT(ReadAll("t2", {"k", "d1", "d2"}),
                IsOkAndHoldsRows({{1, 1, 2}}));
  } else {
    // Unlike production, the emulator eagerly evaluates default columns for
    // each individual mutation. So this transaction violates the NOT NULL
    // constraint on d2. This is a fidelity gap we accept in favor of simplicity
    // of the emulator framework.
    EXPECT_THAT(
        s,
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            testing::HasSubstr(
                "Cannot specify a null value for column: t2.d2 in table: t")));
  }
}

TEST_P(ColumnDefaultValueReadWriteTest, Index) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d1", "d2"}, {1, 1, 1}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{1, 1}}));

  ZETASQL_ASSERT_OK(Update("t", {"k", "d1", "d2"}, {1, 2, 2}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{1, 2}}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d1", "d2"}, {1, 3, 3}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{1, 3}}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "d1", "d2"}, {2, 2, 2}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{2, 2}, {1, 3}}));

  ZETASQL_ASSERT_OK(Replace("t", {"k", "d1", "d2"}, {1, 5, 5}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{2, 2}, {1, 5}}));

  ZETASQL_ASSERT_OK(Replace("t", {"k", "d1", "d2"}, {2, 6, 6}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{1, 5}, {2, 6}}));

  ZETASQL_ASSERT_OK(Delete("t", {Key(2)}));
  EXPECT_THAT(ReadAllWithIndex("t", "tbyd2", {"k", "d2"}),
              IsOkAndHoldsRows({{1, 5}}));

  EXPECT_THAT(Insert("t", {"k", "d1", "d2"}, {2, 5, 5}),
              StatusIs(absl::StatusCode::kAlreadyExists,
                       WhenLowercased(testing::HasSubstr("unique"))));
}

TEST_P(ColumnDefaultValueReadWriteTest, NotNull) {
  EXPECT_THAT(Insert("t", {"k", "d2"}, {1, Null<std::int64_t>()}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       WhenLowercased(testing::HasSubstr("null"))));
}

TEST_P(ColumnDefaultValueReadWriteTest, ForeignKey) {
  EXPECT_THAT(Insert("FK", {"k"}, {3}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Foreign key")));
  ZETASQL_ASSERT_OK(Insert("t", {"k", "d1", "d2"}, {1, 1, 3}));
  ZETASQL_EXPECT_OK(Insert("FK", {"k"}, {3}));
  EXPECT_THAT(Delete("t", {Key(1)}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Foreign key")));
}

TEST_P(ColumnDefaultValueReadWriteTest, DMLsUsingDefaultValues) {
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT INTO t (k, d2) VALUES (1, 1000), (2, 2000)"),
       SqlStatement("UPDATE t SET d1 = 100 WHERE k = 2")}));
  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 100, 101, 2000}}));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("UPDATE t SET d1 = DEFAULT WHERE k = 2")}));
  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 1, 2, 2000}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, DMLsInsertWithDefaultKeyword) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO t (k, d1, d2) VALUES (3, 300, "
                              "DEFAULT), (4, DEFAULT, 400)")}));
  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{3, 300, 301, 2}, {4, 1, 2, 400}}));

  // Try delete:
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM t WHERE d1=1")}));
  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{3, 300, 301, 2}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, InvalidUsesOfDefaultKeyword) {
  absl::string_view error_message =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "DEFAULT is not allowed in this context"
          : "Unexpected keyword DEFAULT";
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT INTO t (k, d2) VALUES (1, 1000), (2, 2000)"),
       SqlStatement("UPDATE t SET d1 = 100 WHERE k = 2")}));
  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 100, 101, 2000}}));

  ASSERT_THAT(CommitDml({SqlStatement("DELETE FROM t WHERE d1=DEFAULT")}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(error_message)));

  ASSERT_THAT(CommitDml({SqlStatement("UPDATE t SET d2 = 4 WHERE d1=DEFAULT")}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(error_message)));

  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{1, 1, 2, 1000}, {2, 100, 101, 2000}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, DMLsInsertWithUserInput) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      "INSERT INTO t2 (k, d1, d2) VALUES (5, 5, 5), (6, 6, 6)")}));
  EXPECT_THAT(Query("SELECT k, d1, d2 FROM t2 ORDER BY k ASC"),
              IsOkAndHoldsRows({{5, 5, 5}, {6, 6, 6}}));

  // Insert .. select, d1 doesn't appear in the column list so it gets
  // its default value = 1,  g1 is computed from d1
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO t (k, d2) SELECT k,  d2 FROM t2")}));
  EXPECT_THAT(Query("SELECT k, d1, g1, d2 FROM t ORDER BY k ASC"),
              IsOkAndHoldsRows({{5, 1, 2, 5}, {6, 1, 2, 6}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, DefaultValueWithUDF) {
  // TODO: b/430093041: add tests for PostgreSQL
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    return;
  }
  ZETASQL_ASSERT_OK(Insert("udf_table", {"string_col"}, {"abc"}));
  ZETASQL_ASSERT_OK(InsertOrUpdate("udf_table", {"string_col"}, {"def"}));
  EXPECT_THAT(ReadAll("udf_table", {"int64_col", "string_col"}),
              IsOkAndHoldsRows({{42, "abc"}, {42, "def"}}));

  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT INTO udf_table (string_col) VALUES ('ghi')")}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      "INSERT OR UPDATE INTO udf_table (string_col) VALUES ('jkl')")}));
  EXPECT_THAT(
      ReadAll("udf_table", {"int64_col", "string_col"}),
      IsOkAndHoldsRows({{42, "abc"}, {42, "def"}, {42, "ghi"}, {42, "jkl"}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, MutationPCT) {
  // Insert two rows. One with a user provided value for ts_default and one
  // that depends on the DEFAULT value.
  ZETASQL_EXPECT_OK(Insert(
      "default_pct", {"k", "ts", "ts_default"},
      {1, cloud::spanner::MakeTimestamp(absl::FromUnixMillis(10)).value(),
       cloud::spanner::MakeTimestamp(absl::FromUnixMillis(10)).value()}));
  ZETASQL_EXPECT_OK(
      Insert("default_pct", {"k", "ts"}, {2, "spanner.commit_timestamp()"}));
  EXPECT_THAT(
      Query("SELECT k from default_pct WHERE ts = ts_default ORDER BY k ASC"),
      IsOkAndHoldsRows({{1}, {2}}));

  // Use InsertOrUpdate to insert a new row.
  ZETASQL_EXPECT_OK(InsertOrUpdate("default_pct", {"k", "ts"},
                           {3, "spanner.commit_timestamp()"}));
  EXPECT_THAT(
      Query("SELECT k from default_pct WHERE ts = ts_default ORDER BY k ASC"),
      IsOkAndHoldsRows({{1}, {2}, {3}}));

  // InsertOrUpdate for an existing row should not trigger DEFAULT.
  ZETASQL_EXPECT_OK(InsertOrUpdate("default_pct", {"k"}, {1}));
  EXPECT_THAT(
      Query("SELECT k, ts, ts_default FROM default_pct WHERE k = 1"),
      IsOkAndHoldsRows(
          {{1, cloud::spanner::MakeTimestamp(absl::FromUnixMillis(10)).value(),
            cloud::spanner::MakeTimestamp(absl::FromUnixMillis(10)).value()}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, DMLsWithPCT) {
  std::string pct_function =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "SPANNER.PENDING_COMMIT_TIMESTAMP"
          : "PENDING_COMMIT_TIMESTAMP";
  std::string timestamp_function =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "TO_TIMESTAMP"
          : "TIMESTAMP_SECONDS";

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      absl::Substitute("INSERT INTO default_pct(k, ts, ts_default) VALUES (1, "
                       "$0(10), $0(10))",
                       timestamp_function))}));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(absl::Substitute(
      "INSERT INTO default_pct(k, ts) VALUES (2, $0())", pct_function))}));

  std::string sql = "INSERT $0 INTO default_pct(k, ts) VALUES (3, $1()) $2";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(
        sql, "", pct_function,
        "ON CONFLICT(k) DO UPDATE SET k = excluded.k, ts = excluded.ts");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", pct_function, "");
  }
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(sql)}));

  sql =
      "INSERT $0 INTO default_pct(k, ts, ts_default) "
      "VALUES (4, $1(), DEFAULT) $2";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "", pct_function,
                           "ON CONFLICT(k) DO UPDATE SET k = excluded.k, ts = "
                           "excluded.ts, ts_default = excluded.ts_default");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", pct_function, "");
  }
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(
      Query("SELECT k from default_pct WHERE ts = ts_default ORDER BY k ASC"),
      IsOkAndHoldsRows({{1}, {2}, {3}, {4}}));

  // InsertOrUpdate for an existing row should not trigger DEFAULT.
  sql = "INSERT $0 INTO default_pct(k) VALUES (1) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(k) DO UPDATE SET k = excluded.k");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "");
  }
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(
      Query("SELECT k, ts, ts_default FROM default_pct WHERE k = 1"),
      IsOkAndHoldsRows(
          {{1, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value(),
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10))
                .value()}}));

  // Explicitly set row 1 to have different ts and ts_default values.
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement(absl::Substitute("UPDATE default_pct SET ts=$0(0), "
                                     "ts_default=$0(100) WHERE k=1",
                                     timestamp_function))}));
  EXPECT_THAT(
      Query("SELECT k from default_pct WHERE ts = ts_default ORDER BY k ASC"),
      IsOkAndHoldsRows({{2}, {3}, {4}}));

  // Use the DEFAULT keyword to set them to the same value again.
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement(absl::Substitute("UPDATE default_pct SET ts=$0(), "
                                     "ts_default = DEFAULT WHERE k=1",
                                     pct_function))}));
  EXPECT_THAT(
      Query("SELECT k from default_pct WHERE ts = ts_default ORDER BY k ASC"),
      IsOkAndHoldsRows({{1}, {2}, {3}, {4}}));
}

TEST_P(ColumnDefaultValueReadWriteTest, ReturningPCT) {
  std::string returning =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "RETURNING"
                                                              : "THEN RETURN";
  // Returning a PCT value via an explicit DEFAULT is not allowed.
  std::vector<ValueRow> result;
  EXPECT_THAT(CommitDmlReturning({SqlStatement(absl::Substitute(
                                     "INSERT INTO default_pct(k, ts_default) "
                                     "VALUES (2, DEFAULT) $0 ts_default",
                                     returning))},
                                 result),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(
                           "The PENDING_COMMIT_TIMESTAMP() function may only "
                           "be used as a value for INSERT or UPDATE")));

  // Returning a PCT value via an implicit DEFAULT is not allowed.
  EXPECT_THAT(CommitDmlReturning(
                  {SqlStatement(absl::Substitute(
                      "INSERT INTO default_pct(k) VALUES (3) $0 ts_default",
                      returning))},
                  result),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(
                           "The PENDING_COMMIT_TIMESTAMP() function may only "
                           "be used as a value for INSERT or UPDATE")));
}

class DefaultPrimaryKeyReadWriteTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("default_primary_key.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectDefaultPrimaryKeyTests, DefaultPrimaryKeyReadWriteTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<DefaultPrimaryKeyReadWriteTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(DefaultPrimaryKeyReadWriteTest, InsertMutations) {
  // A few inserts with default values:
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("t1", {"k1", "a"}, 1, 1),
      MakeInsert("t1", {"a"}, 1),
      MakeInsert("t1", {"k2", "a"}, 1, 1),
  }));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1}}));

  // All columns are present:
  ZETASQL_ASSERT_OK(Insert("t1", {"k1", "k2", "a"}, {2, 200, 2}));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1},
                                {2, 200, 2}}));

  {
    // These inserts fail because the primary key already exist:
    EXPECT_THAT(Insert("t1", {"k1", "a"}, {1, 2}),
                StatusIs(absl::StatusCode::kAlreadyExists));

    EXPECT_THAT(Insert("t1", {"k2", "a"}, {1, 2}),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }
}

TEST_P(DefaultPrimaryKeyReadWriteTest, InsertOrUpdateMutations) {
  // All 3 inserts
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("t1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"a"}, 1),
      MakeInsertOrUpdate("t1", {"k2", "a"}, 1, 1),
  }));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1}}));

  // An insert and an update
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("t1", {"k1", "a"}, 3, 3),
      MakeInsertOrUpdate("t1", {"k1", "a"}, 3, 300),
  }));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1},
                                {3, 2, 300}}));

  // All columns are present:
  ZETASQL_ASSERT_OK(InsertOrUpdate("t1", {"k1", "k2", "a"}, {2, 200, 2}));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1},
                                {Null<std::int64_t>(), 2, 1},
                                {1, 2, 1},
                                {2, 200, 2},
                                {3, 2, 300}}));
}

TEST_P(DefaultPrimaryKeyReadWriteTest, UpdateMutations) {
  // Insert a fw rows and update one
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("t1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"k2", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"k1", "k2", "a"}, 1, 1, 1),
      MakeUpdate("t1", {"k1", "k2", "a"}, 1, 2, 100),
  }));
  EXPECT_THAT(
      ReadAll("t1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 100}}));

  // This update fail because all PK columns must be present:
  EXPECT_THAT(Update("t1", {"k1", "a"}, {1, 1000}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("must have a specific value")));
  EXPECT_THAT(
      ReadAll("t1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 100}}));

  ZETASQL_ASSERT_OK(Update("t1", {"k1", "k2", "a"}, {1, 1, 200}));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows(
                  {{Null<std::int64_t>(), 1, 1}, {1, 1, 200}, {1, 2, 100}}));
}

TEST_P(DefaultPrimaryKeyReadWriteTest, DeleteMutations) {
  // Insert a few rows and delete one
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("t1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"k2", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"k1", "k2", "a"}, 1, 1, 1),
      MakeDelete("t1", Singleton(1, 1)),
  }));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 2, 1}}));

  // This delete fails because all PK columns must be present:
  EXPECT_THAT(Delete("t1", Key(1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(ReadAll("t1", {"k1", "k2", "a"}),
              IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 2, 1}}));
}

TEST_P(DefaultPrimaryKeyReadWriteTest, ReplaceMutations) {
  // Insert a few rows and replace one
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("t1", {"k1", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"k2", "a"}, 1, 1),
      MakeInsertOrUpdate("t1", {"k1", "k2", "a"}, 1, 1, 1),
      MakeReplace("t1", {"k1", "a"}, 1, 100),
  }));
  EXPECT_THAT(
      ReadAll("t1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 100}}));

  // The following replace should trigger the default value of k2 to be
  // generated, hence replacing the row (1, 2, 100) with (1, 2, 200).
  // The row (1, 1, 1) should remain the same.
  ZETASQL_ASSERT_OK(Replace("t1", {"k1", "a"}, {1, 200}));
  EXPECT_THAT(
      ReadAll("t1", {"k1", "k2", "a"}),
      IsOkAndHoldsRows({{Null<std::int64_t>(), 1, 1}, {1, 1, 1}, {1, 2, 200}}));
}

TEST_P(DefaultPrimaryKeyReadWriteTest, WithDependentGeneratedColumn) {
  // All 3 inserts
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("t2", {"id1", "a"}, 1, 1),
      MakeInsertOrUpdate("t2", {"a"}, 1),
      MakeInsertOrUpdate("t2", {"id2", "a"}, 1, 1),
  }));
  EXPECT_THAT(ReadAll("t2", {"id1", "id2", "g1", "a"}),
              IsOkAndHoldsRows({{1, Null<std::int64_t>(), 2, 1},
                                {100, Null<std::int64_t>(), 200, 1},
                                {100, 1, 200, 1}}));

  // An insert and an update
  ZETASQL_ASSERT_OK(Commit({
      MakeInsertOrUpdate("t2", {"id2", "a"}, 3, 3),
      MakeInsertOrUpdate("t2", {"id2", "a"}, 3, 300),
  }));
  EXPECT_THAT(ReadAll("t2", {"id1", "id2", "g1", "a"}),
              IsOkAndHoldsRows({{1, Null<std::int64_t>(), 2, 1},
                                {100, Null<std::int64_t>(), 200, 1},
                                {100, 1, 200, 1},
                                {100, 3, 200, 300}}));
}

TEST_P(DefaultPrimaryKeyReadWriteTest, DMLs) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO t2 (id2, a) VALUES (1, 1)")}));
  EXPECT_THAT(Query("SELECT id1, id2, g1, a FROM t2 ORDER BY id2 ASC"),
              IsOkAndHoldsRows({{100, 1, 200, 1}}));

  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO t2 (id1, id2, a) VALUES "
                              "(DEFAULT, 2, 2), (3, 3, 3)")}));
  EXPECT_THAT(
      Query("SELECT id1, id2, g1, a FROM t2 ORDER BY id2 ASC"),
      IsOkAndHoldsRows({{100, 1, 200, 1}, {100, 2, 200, 2}, {3, 3, 6, 3}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
