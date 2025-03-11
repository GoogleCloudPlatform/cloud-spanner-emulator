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
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "google/cloud/spanner/value.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::zetasql_base::testing::StatusIs;

class SequenceReadWriteTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  SequenceReadWriteTest()
      : feature_flags_(
            {.enable_postgresql_interface = true,
             .enable_bit_reversed_positive_sequences = true,
             .enable_bit_reversed_positive_sequences_postgresql = true}) {}

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("sequence.test");
  }

 protected:
  absl::StatusOr<int64_t> GetCurrentSequenceState(const std::string& name) {
    std::string query;
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      query = "SELECT spanner.GET_INTERNAL_SEQUENCE_STATE('$0')";
    } else {
      query = "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE $0)";
    }
    ZETASQL_ASSIGN_OR_RETURN(std::vector<ValueRow> query_result,
                     Query(absl::Substitute(query, name)));
    ZETASQL_RET_CHECK(query_result[0].values()[0].get<int64_t>().ok());
    return *(query_result[0].values()[0].get<int64_t>());
  }

  void InsertThreeRowsWithDML() {
    ZETASQL_ASSERT_OK(CommitDml(
        {SqlStatement("INSERT INTO users(name, age) VALUES ('Levin', 27), "
                      "('Mark', 32), ('Douglas', 31)")}));
    EXPECT_THAT(
        Query("SELECT name, age FROM users ORDER BY age ASC"),
        IsOkAndHoldsRows({{"Levin", 27}, {"Douglas", 31}, {"Mark", 32}}));
    // The first 3 ids should be large int64_t numbers
    EXPECT_THAT(Query("SELECT count(id) FROM users WHERE id > 100000"),
                IsOkAndHoldsRows({3}));
  }

  void ExpectEmptyInternalState(const std::string& name) {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      EXPECT_THAT(
          Query("SELECT spanner.GET_INTERNAL_SEQUENCE_STATE('mysequence')"),
          IsOkAndHoldsRows({Null<std::int64_t>()}));
    } else {
      EXPECT_THAT(
          Query("SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE mysequence)"),
          IsOkAndHoldsRows({Null<std::int64_t>()}));
    }
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectSequenceReadWriteTests, SequenceReadWriteTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<SequenceReadWriteTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(SequenceReadWriteTest, Dml_Basic) {
  ExpectEmptyInternalState("mysequence");
  InsertThreeRowsWithDML();

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
        R"(UPDATE users SET age =
                nextval('mysequence')
                WHERE name = 'Levin' )")}));
    EXPECT_THAT(
        Query("SELECT (spanner.get_internal_sequence_state('mysequence') >= 4) "
              "is true"),
        IsOkAndHoldsRows({{true}}));
  } else {
    ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
        R"(UPDATE users SET age =
                GET_NEXT_SEQUENCE_VALUE(SEQUENCE mysequence)
                WHERE name = "Levin" )")}));
    EXPECT_THAT(
        Query("SELECT (get_internal_sequence_state(sequence mysequence) >= 4) "
              "is true"),
        IsOkAndHoldsRows({{true}}));
  }
}

TEST_P(SequenceReadWriteTest, Mutation_Basic) {
  ExpectEmptyInternalState("mysequence");

  // Insert 3 rows
  ZETASQL_ASSERT_OK(MultiInsert("users", {"name", "age"},
                        {{"Levin", 27}, {"Mark", 32}, {"Douglas", 31}}));
  EXPECT_THAT(ReadAll("users", {"name", "age"}),
              IsOkAndHoldsUnorderedRows(
                  {{"Levin", 27}, {"Mark", 32}, {"Douglas", 31}}));

  // Insert another 3 rows using InsertOrUpdate
  ZETASQL_ASSERT_OK(MultiInsertOrUpdate("users", {"name", "age"},
                                {{"Alex", 10}, {"Bob", 11}, {"Carol", 12}}));
  EXPECT_THAT(ReadAll("users", {"name", "age"}),
              IsOkAndHoldsUnorderedRows({{"Alex", 10},
                                         {"Bob", 11},
                                         {"Carol", 12},
                                         {"Levin", 27},
                                         {"Mark", 32},
                                         {"Douglas", 31}}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        Query("SELECT (spanner.get_internal_sequence_state('mysequence') >=6) "
              "is true"),
        IsOkAndHoldsRows({{true}}));
  } else {
    EXPECT_THAT(
        Query("SELECT (get_internal_sequence_state(sequence mysequence) >=6) "
              "is true"),
        IsOkAndHoldsRows({{true}}));
  }
}

TEST_P(SequenceReadWriteTest, WithThenReturn) {
  ExpectEmptyInternalState("mysequence");

  // Insert 3 rows with value = 1, 2, 3
  // Insert THEN RETURN
  std::vector<ValueRow> returning_result;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_EXPECT_OK(CommitDmlReturning(
        {SqlStatement("INSERT INTO users(name, age) VALUES "
                      "('Levin', 27), ('Douglas', 31), ('Mark', 32)"
                      "RETURNING id;")},
        returning_result));
  } else {
    ZETASQL_EXPECT_OK(CommitDmlReturning(
        {SqlStatement("INSERT INTO users(name, age) VALUES "
                      "('Levin', 27), ('Douglas', 31), ('Mark', 32)"
                      "THEN RETURN id;")},
        returning_result));
  }

  EXPECT_EQ(returning_result.size(), 3);
  // The returning result should have 3 rows, each row has one value.
  EXPECT_GT(*(returning_result[0].values()[0].get<std::int64_t>()), 0);
  EXPECT_GT(*(returning_result[1].values()[0].get<std::int64_t>()), 0);
  EXPECT_GT(*(returning_result[2].values()[0].get<std::int64_t>()), 0);

  EXPECT_THAT(Query("SELECT id FROM users"),
              IsOkAndHoldsUnorderedRows(returning_result));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> query_result,
                       Query("SELECT counter FROM users ORDER BY age ASC"));
  EXPECT_EQ(query_result.size(), 3);

  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t counter, GetCurrentSequenceState("mysequence"));
  EXPECT_LE(*(query_result[0].values()[0].get<std::int64_t>()), counter);
  EXPECT_LE(*(query_result[1].values()[0].get<std::int64_t>()), counter);
  EXPECT_LE(*(query_result[2].values()[0].get<std::int64_t>()), counter);
}

TEST_P(SequenceReadWriteTest, WithCustomStartWithCounter) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
       ALTER SEQUENCE mysequence RESTART COUNTER 50001;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
       ALTER SEQUENCE mysequence SET OPTIONS (
        start_with_counter = 50001
       )
    )"}));
  }

  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<ValueRow> query_result,
      Query("SELECT counter, br_id FROM users ORDER BY age ASC"));
  EXPECT_EQ(query_result.size(), 3);

  // counter values retrieved from the rows should be > 50000
  EXPECT_GT(*(query_result[0].values()[0].get<std::int64_t>()), 50000);
  EXPECT_GT(*(query_result[1].values()[0].get<std::int64_t>()), 50000);
  EXPECT_GT(*(query_result[2].values()[0].get<std::int64_t>()), 50000);
  // br_id values retrieved from the rows should be > 50000
  EXPECT_GT(*(query_result[0].values()[1].get<std::int64_t>()), 50000);
  EXPECT_GT(*(query_result[1].values()[1].get<std::int64_t>()), 50000);
  EXPECT_GT(*(query_result[2].values()[1].get<std::int64_t>()), 50000);
}

TEST_P(SequenceReadWriteTest, WithSmallHalfSkippedRange) {
  // Skip half of the positive int64_t range, from 0 - 2^62
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SKIP RANGE 0 4611686018427387904;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          skip_range_min = 0,
          skip_range_max = 4611686018427387904
        )
    )"}));
  }

  ExpectEmptyInternalState("mysequence");

  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> query_result,
                       Query("SELECT id FROM users ORDER BY age ASC"));
  EXPECT_EQ(query_result.size(), 3);

  int64_t skip_range_max = 4611686018427387904;

  // values should be out of the skipped range.
  EXPECT_GT(*(query_result[0].values()[0].get<std::int64_t>()), skip_range_max);
  EXPECT_GT(*(query_result[1].values()[0].get<std::int64_t>()), skip_range_max);
  EXPECT_GT(*(query_result[2].values()[0].get<std::int64_t>()), skip_range_max);
}

TEST_P(SequenceReadWriteTest, WithBigHalfSkippedRange) {
  // Skip half of the positive int64_t range, from 2^62 - 2^63-1
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence
            SKIP RANGE 4611686018427387904 9223372036854775807;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          skip_range_min = 4611686018427387904,
          skip_range_max = 9223372036854775807
        )
    )"}));
  }

  ExpectEmptyInternalState("mysequence");

  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> query_result,
                       Query("SELECT id FROM users ORDER BY age ASC"));
  EXPECT_EQ(query_result.size(), 3);

  int64_t skip_range_min = 4611686018427387904;

  // values should be out of the skipped range, but still positive
  EXPECT_LT(*(query_result[0].values()[0].get<std::int64_t>()), skip_range_min);
  EXPECT_LT(*(query_result[1].values()[0].get<std::int64_t>()), skip_range_min);
  EXPECT_LT(*(query_result[2].values()[0].get<std::int64_t>()), skip_range_min);

  EXPECT_GT(*(query_result[0].values()[0].get<std::int64_t>()), 0);
  EXPECT_GT(*(query_result[1].values()[0].get<std::int64_t>()), 0);
  EXPECT_GT(*(query_result[2].values()[0].get<std::int64_t>()), 0);
}

TEST_P(SequenceReadWriteTest, SkippedRangeExhaustsSequence) {
  // Skip the entire positive int64_t range, from 0 - 2^63-1, the sequence
  // API should fail fast.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence
            SKIP RANGE 0 9223372036854775807;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          skip_range_min = 0,
          skip_range_max = 9223372036854775807
        )
    )"}));
  }

  ExpectEmptyInternalState("mysequence");

  ASSERT_THAT(CommitDml({SqlStatement(
                  "INSERT INTO users(name, age) VALUES ('Levin', 27), "
                  "('Mark', 32), ('Douglas', 31)")}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("Sequence mysequence is exhausted")));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t counter, GetCurrentSequenceState("mysequence"));
  // Check that we have tried retrieving the values, but can't find a valid one.
  EXPECT_GT(counter, 1);

  // Now remove the skipped range, the sequence should see the new schema change
  // and produces valid values.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SKIP RANGE 0 0;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          skip_range_min = NULL,
          skip_range_max = NULL
        )
    )"}));
  }
  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<ValueRow> query_result,
      Query("SELECT counter, br_id FROM users ORDER BY age ASC"));
  EXPECT_EQ(query_result.size(), 3);

  // counter values retrieved from the rows should be larger than or equals to
  // the counter retrieved before, indicating that the sequence counter is
  // moving ahead.
  EXPECT_GE(*(query_result[0].values()[0].get<std::int64_t>()), counter);
  EXPECT_GE(*(query_result[1].values()[0].get<std::int64_t>()), counter);
  EXPECT_GE(*(query_result[2].values()[0].get<std::int64_t>()), counter);
}

// Verifies that the cache invalidation logic works correctly every time the
// sequence is altered
TEST_P(SequenceReadWriteTest, AlterSequenceMultipleTimes) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence
            SKIP RANGE 0 1000;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          skip_range_min = 0,
          skip_range_max = 1000
        )
    )"}));
  }

  ExpectEmptyInternalState("mysequence");

  InsertThreeRowsWithDML();

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence
            RESTART COUNTER 10001;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          start_with_counter = 10001
        )
    )"}));
  }

  // Insert another 3 rows
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT INTO users(name, age) VALUES ('Alex', 5), "
                    "('Bob', 6), ('Carol', 7)")}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> query_result,
                       Query("SELECT counter FROM users ORDER BY counter ASC"));
  EXPECT_EQ(query_result.size(), 6);

  // The first 3 counters should be < 10000, the latter 3 should be > 10000
  EXPECT_LT(*(query_result[0].values()[0].get<std::int64_t>()), 10000);
  EXPECT_LT(*(query_result[1].values()[0].get<std::int64_t>()), 10000);
  EXPECT_LT(*(query_result[2].values()[0].get<std::int64_t>()), 10000);
  EXPECT_GT(*(query_result[3].values()[0].get<std::int64_t>()), 10000);
  EXPECT_GT(*(query_result[4].values()[0].get<std::int64_t>()), 10000);
  EXPECT_GT(*(query_result[5].values()[0].get<std::int64_t>()), 10000);
}

TEST_P(SequenceReadWriteTest, MovesStartWithCounterBackward) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence
            RESTART COUNTER 10000;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          start_with_counter = 10000
        )
    )"}));
  }

  ExpectEmptyInternalState("mysequence");

  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t counter, GetCurrentSequenceState("mysequence"));
  // Check that we have moved the counter at least 3 times
  EXPECT_GT(counter, 10000);

  // Moves the counter backward
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence
            RESTART COUNTER 1;
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER SEQUENCE mysequence SET OPTIONS (
          start_with_counter = 1
        )
    )"}));
  }

  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT INTO users(name, age) VALUES ('Alex', 5), "
                    "('Bob', 6), ('Carol', 7)")}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<ValueRow> query_result,
      Query("SELECT counter, br_id FROM users WHERE age < 10"));
  EXPECT_EQ(query_result.size(), 3);

  // counter and br_id of the new rows should be smaller than the old counter.
  EXPECT_LT(*(query_result[0].values()[0].get<std::int64_t>()), 10000);
  EXPECT_LT(*(query_result[1].values()[0].get<std::int64_t>()), 10000);
  EXPECT_LT(*(query_result[2].values()[0].get<std::int64_t>()), 10000);

  EXPECT_LT(*(query_result[0].values()[1].get<std::int64_t>()), 10000);
  EXPECT_LT(*(query_result[1].values()[1].get<std::int64_t>()), 10000);
  EXPECT_LT(*(query_result[2].values()[1].get<std::int64_t>()), 10000);
}

// Verifies that a deleted and re-created sequence works as expected.
TEST_P(SequenceReadWriteTest, DropAndRecreateSequence) {
  ExpectEmptyInternalState("mysequence");

  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t counter, GetCurrentSequenceState("mysequence"));
  // Check that we have moved the counter at least 3 times
  EXPECT_GT(counter, 3);

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER TABLE users DROP CONSTRAINT id_gt_0
    )",
                          R"(
      ALTER TABLE users DROP CONSTRAINT counter_gt_br_id
    )",
                          R"(
      ALTER TABLE users DROP CONSTRAINT br_id_true
    )",
                          R"(
      ALTER TABLE users ALTER COLUMN id DROP DEFAULT
    )",
                          R"(
      ALTER TABLE users ALTER COLUMN counter DROP DEFAULT
    )",
                          R"(
        DROP SEQUENCE mysequence
    )"}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        CREATE SEQUENCE mysequence BIT_REVERSED_POSITIVE
      )",
                            R"(
          ALTER TABLE users ALTER COLUMN id SET DEFAULT nextval('mysequence')
      )",
                            R"(
          ALTER TABLE users ALTER COLUMN counter SET DEFAULT
              spanner.get_internal_sequence_state('mysequence')
      )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        CREATE SEQUENCE mysequence OPTIONS (
          sequence_kind = "bit_reversed_positive"
        )
      )",
                            R"(
          ALTER TABLE users ALTER COLUMN id SET DEFAULT
            (GET_NEXT_SEQUENCE_VALUE(SEQUENCE mysequence))
      )",
                            R"(
          ALTER TABLE users ALTER COLUMN counter SET DEFAULT
            (GET_INTERNAL_SEQUENCE_STATE(SEQUENCE mysequence))
      )"}));
  }

  // The new sequence should have empty state
  ExpectEmptyInternalState("mysequence");
}

// Verifies that backfill works as expected
TEST_P(SequenceReadWriteTest, AddColumn) {
  ExpectEmptyInternalState("mysequence");

  InsertThreeRowsWithDML();

  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t counter, GetCurrentSequenceState("mysequence"));
  // Check that we have moved the counter at least 3 times
  EXPECT_GT(counter, 3);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER TABLE users ADD COLUMN col1 bigint DEFAULT nextval('mysequence')
    )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER TABLE users ADD COLUMN col1 INT64
            DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE mysequence))
    )"}));
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t new_counter,
                       GetCurrentSequenceState("mysequence"));
  EXPECT_GE(new_counter, counter);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> query_result,
                       Query("SELECT col1 FROM users"));
  EXPECT_EQ(query_result.size(), 3);

  EXPECT_GE(*(query_result[0].values()[0].get<std::int64_t>()), counter);
  EXPECT_GE(*(query_result[1].values()[0].get<std::int64_t>()), counter);
  EXPECT_GE(*(query_result[2].values()[0].get<std::int64_t>()), counter);
}

TEST_P(SequenceReadWriteTest,
       GetNextSequenceValueCannotBeUsedInReadOnlyTransaction) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(QueryTransaction(Transaction(Transaction::ReadOnlyOptions()),
                                 "SELECT nextval('mysequence')"),
                StatusIs(absl::StatusCode::kInvalidArgument));

    EXPECT_THAT(
        QuerySingleUseTransaction(
            Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}},
            SqlStatement("SELECT nextval('mysequence')")),
        StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(
        QueryTransaction(Transaction(Transaction::ReadOnlyOptions()),
                         "SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE mysequence)"),
        StatusIs(absl::StatusCode::kInvalidArgument));

    EXPECT_THAT(
        QuerySingleUseTransaction(
            Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}},
            SqlStatement(
                "SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE mysequence)")),
        StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST_P(SequenceReadWriteTest,
       GetNextSequenceValueCanBeUsedInReadWriteTransaction) {
  std::vector<ValueRow> value_row;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        value_row,
        QueryTransaction(Transaction(Transaction::ReadWriteOptions()),
                         "SELECT nextval('mysequence')"));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        value_row, QueryTransaction(
                       Transaction(Transaction::ReadWriteOptions()),
                       "SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE mysequence)"));
  }
}

TEST_P(SequenceReadWriteTest,
       GetInternalSequenceStateCanBeUsedInAllTypesOfTransaction) {
  std::vector<ValueRow> value_row;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        value_row,
        QueryTransaction(
            Transaction(Transaction::ReadOnlyOptions()),
            "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE mysequence)"));

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        value_row,
        QueryTransaction(
            Transaction(Transaction::ReadWriteOptions()),
            "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE mysequence)"));

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        value_row,
        QuerySingleUseTransaction(
            Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}},
            SqlStatement(
                "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE mysequence)")));
  }
}

TEST_P(SequenceReadWriteTest,
       GetInternalSequenceStateInvalidTimestampReadOnlyTransaction) {
  std::vector<ValueRow> value_row;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        QuerySingleUseTransaction(
            Transaction::SingleUseOptions(MakeMinTimestamp()),
            SqlStatement(
                "SELECT spanner.get_internal_sequence_state('mysequence')")),
        StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(
        QuerySingleUseTransaction(
            Transaction::SingleUseOptions(MakeMinTimestamp()),
            SqlStatement(
                "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE mysequence)")),
        StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST_P(SequenceReadWriteTest, InsertAfterIfNotExistsSchemaUpdate) {
  std::vector<std::string> gsql_ddl_statements = {
      R"(
      CREATE SEQUENCE IF NOT EXISTS test_seq OPTIONS (
        sequence_kind='bit_reversed_positive')
      )",
      R"(
      CREATE TABLE IF NOT EXISTS test_table (
          id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE test_seq)),
          value INT64 NOT NULL
      ) PRIMARY KEY(id))"};
  std::vector<std::string> pg_ddl_statements = {
      R"(
      CREATE SEQUENCE IF NOT EXISTS test_seq BIT_REVERSED_POSITIVE
      )",
      R"(
      CREATE TABLE IF NOT EXISTS test_table (
          id bigint default (nextval('test_seq')),
          value bigint,
          PRIMARY KEY(id)
      ))"};

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema(pg_ddl_statements));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema(gsql_ddl_statements));
  }

  ZETASQL_ASSERT_OK(MultiInsert("test_table", {"value"}, {{1}, {2}, {3}}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema(pg_ddl_statements));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema(gsql_ddl_statements));
  }

  ZETASQL_ASSERT_OK(MultiInsert("test_table", {"value"}, {{4}, {5}, {6}}));
}

TEST_P(SequenceReadWriteTest, SequenceFunctionInWhereClause) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema({"CREATE SEQUENCE seq bit_reversed_positive",
                            R"(
    CREATE TABLE test_table (
      id BIGINT DEFAULT (nextval('seq')),
      value BIGINT,
      PRIMARY KEY(id)
    ))"}));
    ZETASQL_ASSERT_OK(CommitDml(
        {SqlStatement("INSERT INTO test_table(value) VALUES (1), (2), (3)")}));
    // Check that counter has incremented at least 3 times.
    EXPECT_THAT(Query("select spanner.GET_INTERNAL_SEQUENCE_STATE('seq') >= 3"),
                IsOkAndHoldsRows({{true}}));
    // GET_NEXT_SEQUENCE_VALUE is only allowed in a read-write transaction.
    EXPECT_THAT(Query("SELECT nextval('seq')"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::ContainsRegex(
                             "(only allowed in read-write transactions|can "
                             "only be used in a read-write)")));
    EXPECT_THAT(Query(R"(
        SELECT 1 FROM test_table
        WHERE (nextval('seq')) IN (1,2,3))")
                    .status()
                    .message(),
                testing::ContainsRegex("(not supported in a WHERE clause|can "
                                       "only be used in a read-write)"));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema({R"(
    CREATE SEQUENCE seq OPTIONS (sequence_kind = 'bit_reversed_positive'))",
                            R"(
    CREATE TABLE test_table (
      id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq)),
      value INT64
    ) PRIMARY KEY(id))"}));
    ZETASQL_ASSERT_OK(CommitDml(
        {SqlStatement("INSERT INTO test_table(value) VALUES (1), (2), (3)")}));
    // Check that counter has incremented at least 3 times.
    EXPECT_THAT(Query("select GET_INTERNAL_SEQUENCE_STATE(SEQUENCE seq) >= 3"),
                IsOkAndHoldsRows({{true}}));
    // GET_NEXT_SEQUENCE_VALUE is only allowed in a read-write transaction.
    EXPECT_THAT(Query("SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq)"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::ContainsRegex(
                             "(only allowed in read-write transactions|can "
                             "only be used in a read-write)")));
    EXPECT_THAT(Query(R"(
        SELECT 1 FROM test_table
        WHERE (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq)) IN (1,2,3))")
                    .status()
                    .message(),
                testing::ContainsRegex("(not supported in a WHERE clause|can "
                                       "only be used in a read-write)"));
  }
}

TEST_P(SequenceReadWriteTest, DropTableShouldNotImpactSequenceInternalState) {
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    ZETASQL_ASSERT_OK(
        UpdateSchema({"ALTER DATABASE db SET OPTIONS (default_sequence_kind = "
                      "'bit_reversed_positive')",
                      "CREATE TABLE test (id INT64 AUTO_INCREMENT primary key, "
                      "val STRING(MAX))"}));
    ZETASQL_ASSERT_OK(
        CommitDml({SqlStatement("INSERT INTO test (val) VALUES ('test-1')"),
                   SqlStatement("INSERT INTO test (val) VALUES ('test-2')"),
                   SqlStatement("INSERT INTO test (val) VALUES ('test-3')")}));

    // Check that counter has incremented at least 3 times.
    EXPECT_THAT(Query("select get_table_column_identity_state('test.id') >= 4"),
                IsOkAndHoldsRows({{true}}));

    ZETASQL_ASSERT_OK(UpdateSchema(
        {"CREATE TABLE parents (id INT64 AUTO_INCREMENT PRIMARY KEY,deleted_at "
         "TIMESTAMP)",
         "CREATE INDEX IF NOT EXISTS idx_parents_deleted_at ON parents "
         "(deleted_at)"}));

    // This is expected to fail because the index is not dropped.
    EXPECT_THAT(UpdateSchema({"drop table parents"}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         testing::ContainsRegex(
                             "Cannot drop table parents with indices: "
                             "idx_parents_deleted_at")));

    EXPECT_THAT(Query("select get_table_column_identity_state('test.id') >= 4"),
                IsOkAndHoldsRows({{true}}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema(
        {"alter database db set spanner.default_sequence_kind = "
         "'bit_reversed_positive'",
         R"(create table test (id serial primary key, val text))"}));
    ZETASQL_ASSERT_OK(
        CommitDml({SqlStatement("insert into test (val) values ('test-1')"),
                   SqlStatement("insert into test (val) values ('test-2')"),
                   SqlStatement("insert into test (val) values ('test-3')")}));

    // Check that counter has incremented at least 3 times.
    EXPECT_THAT(
        Query("select spanner.get_table_column_identity_state('test.id') >= 4"),
        IsOkAndHoldsRows({{true}}));

    ZETASQL_ASSERT_OK(UpdateSchema(
        {"create table parents (id bigserial,deleted_at timestamptz,primary "
         "key "
         "(id))",
         "create index if not exists idx_parents_deleted_at ON parents "
         "(deleted_at)"}));

    // This is expected to fail because the index is not dropped.
    EXPECT_THAT(UpdateSchema({"drop table parents"}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         testing::ContainsRegex(
                             "Cannot drop table parents with indices: "
                             "idx_parents_deleted_at")));

    EXPECT_THAT(
        Query("select spanner.get_table_column_identity_state('test.id') >= 4"),
        IsOkAndHoldsRows({{true}}));
  }
}
}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
