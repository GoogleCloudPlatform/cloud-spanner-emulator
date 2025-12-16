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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class SelectForUpdateTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    return SetSchemaFromFile("select_for_update.test");
  }

 protected:
  void PopulateDatabase() {
    ZETASQL_EXPECT_OK(MultiInsert("users", {"user_id", "name", "age"},
                          {{1, "Douglas Adams", 49},
                           {2, "Suzanne Collins", 61},
                           {3, "J.R.R. Tolkien", 81}}));

    ZETASQL_EXPECT_OK(MultiInsert("threads", {"user_id", "thread_id", "starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));

    ZETASQL_EXPECT_OK(MultiInsert("messages",
                          {"user_id", "thread_id", "message_id", "subject"},
                          {{1, 1, 1, "a code review"},
                           {1, 1, 2, "Re: a code review"},
                           {1, 2, 1, "Congratulations Douglas"},
                           {1, 3, 1, "Reminder to write feedback"},
                           {1, 4, 1, "Meeting this week"},
                           {2, 1, 1, "Lunch today?"},
                           {2, 2, 1, "Suzanne Collins will be absent"},
                           {3, 1, 1, "Interview Notification"}}));
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectSelectForUpdateTests, SelectForUpdateTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<SelectForUpdateTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(SelectForUpdateTest, InvalidInReadOnlyTransaction) {
  auto txn = Transaction(Transaction::ReadOnlyOptions());

  EXPECT_THAT(
      QueryTransaction(txn,
                       "SELECT age FROM users WHERE user_id = 2 FOR UPDATE"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr(
                   "FOR UPDATE is not supported in this transaction type")));
}

TEST_P(SelectForUpdateTest, OtherLockModesUnsupported) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        Query("SELECT age FROM users WHERE user_id = 2 FOR SHARE"),
        StatusIs(testing::AnyOf(absl::StatusCode::kInvalidArgument,
                                absl::StatusCode::kUnimplemented),
                 testing::HasSubstr("Statements with locking clauses other "
                                    "than FOR UPDATE are not supported")));
  } else {
    EXPECT_THAT(Query("SELECT age FROM users WHERE user_id = 2 FOR SHARE"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::HasSubstr("Syntax error:")));
  }
}

TEST_P(SelectForUpdateTest, CombiningWithStatementLockHintIsInvalid) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  std::string lock_hint =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "/*@lock_scanned_ranges=exclusive*/"
          : "@{lock_scanned_ranges=EXCLUSIVE}";
  EXPECT_THAT(
      QueryTransaction(
          txn, absl::Substitute(
                   "$0SELECT age FROM users WHERE user_id = 2 FOR UPDATE",
                   lock_hint)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr("FOR UPDATE cannot be combined with "
                                  "statement-level lock hints")));
}

TEST_P(SelectForUpdateTest, SelectAndWhere) {
  PopulateDatabase();
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // The following query is valid in PG but not in GSQL.
  absl::StatusCode exp_status =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? absl::StatusCode::kOk
          : absl::StatusCode::kInvalidArgument;
  EXPECT_THAT(QueryTransaction(txn, "SELECT 1 FOR UPDATE"),
              StatusIs(exp_status));

  EXPECT_THAT(
      QueryTransaction(txn, "SELECT user_id, name FROM users FOR UPDATE"),
      IsOkAndHoldsUnorderedRows({{1, "Douglas Adams"},
                                 {2, "Suzanne Collins"},
                                 {3, "J.R.R. Tolkien"}}));

  EXPECT_THAT(
      QueryTransaction(
          txn, "SELECT user_id, name FROM users WHERE user_id = 1 FOR UPDATE"),
      IsOkAndHoldsUnorderedRows({{1, "Douglas Adams"}}));
}

TEST_P(SelectForUpdateTest, WithStatements) {
  PopulateDatabase();
  auto txn = Transaction(Transaction::ReadWriteOptions());

  auto expected = std::vector<ValueRow>(
      {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}});

  // Scoped to CTE subquery.
  EXPECT_THAT(QueryTransaction(txn, R"sql(
          WITH t1 AS (SELECT user_id, name FROM users FOR UPDATE)
          SELECT * FROM t1)sql"),
              IsOkAndHoldsUnorderedRows(expected));

  // Scoped to outer query.
  EXPECT_THAT(QueryTransaction(txn, R"sql(
          WITH t1 AS (SELECT user_id, name FROM users)
          SELECT * FROM t1 FOR UPDATE)sql"),
              IsOkAndHoldsUnorderedRows(expected));
}

TEST_P(SelectForUpdateTest, Subqueries) {
  PopulateDatabase();
  auto txn = Transaction(Transaction::ReadWriteOptions());

  auto expected = std::vector<ValueRow>({{"Reminder to write feedback", true},
                                         {"Re: a code review", true},
                                         {"Meeting this week", false},
                                         {"Congratulations Douglas", true},
                                         {"a code review", true}});

  // Scoped to subquery.
  EXPECT_THAT(QueryTransaction(txn, R"sql(
          SELECT t2.subject, t1.starred FROM (
             SELECT user_id, thread_id, starred
             FROM threads
             WHERE user_id = 1
             FOR UPDATE
          ) AS t1
          JOIN messages AS t2
            ON t1.user_id = t2.user_id AND t1.thread_id = t2.thread_id
          )sql"),
              IsOkAndHoldsUnorderedRows(expected));

  // Scoped to outer query.
  EXPECT_THAT(QueryTransaction(txn, R"sql(
          SELECT t2.subject, t1.starred FROM (
             SELECT user_id, thread_id, starred
             FROM threads
             WHERE user_id = 1
          ) AS t1
          JOIN messages AS t2
            ON t1.user_id = t2.user_id AND t1.thread_id = t2.thread_id
          FOR UPDATE
          )sql"),
              IsOkAndHoldsUnorderedRows(expected));
}

TEST_P(SelectForUpdateTest, SetOperations) {
  PopulateDatabase();
  auto txn = Transaction(Transaction::ReadWriteOptions());

  auto expected_gsql_rows = std::vector<ValueRow>({{2}, {2}, {1}, {3}, {1}});
  auto expected_pg_status =
      StatusIs(testing::AnyOf(absl::StatusCode::kInvalidArgument,
                              absl::StatusCode::kUnimplemented),
               testing::HasSubstr(
                   "FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT"));

  // Scoped to LHS query.
  std::string sql = R"sql(
          (SELECT thread_id FROM messages WHERE message_id > 1 FOR UPDATE)
          UNION ALL
          SELECT thread_id FROM threads WHERE starred = true
          )sql";
  // In PG, set operations can't be combined with FOR UPDATE.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(QueryTransaction(txn, sql), expected_pg_status);
  } else {
    EXPECT_THAT(QueryTransaction(txn, sql),
                IsOkAndHoldsUnorderedRows(expected_gsql_rows));
  }

  // Scoped to RHS query.
  sql = R"sql(
          SELECT thread_id FROM messages WHERE message_id > 1
          UNION ALL
          (SELECT thread_id FROM threads WHERE starred = true FOR UPDATE)
          )sql";
  // In PG, set operations can't be combined with FOR UPDATE.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(QueryTransaction(txn, sql), expected_pg_status);
  } else {
    EXPECT_THAT(QueryTransaction(txn, sql),
                IsOkAndHoldsUnorderedRows(expected_gsql_rows));
  }

  // Scoped to both LHS and RHS queries.
  sql = R"sql(
          SELECT thread_id FROM messages WHERE message_id > 1
          UNION ALL
          SELECT thread_id FROM threads WHERE starred = true
          FOR UPDATE
          )sql";
  // In PG, set operations can't be combined with FOR UPDATE.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(QueryTransaction(txn, sql), expected_pg_status);
  } else {
    EXPECT_THAT(QueryTransaction(txn, sql),
                IsOkAndHoldsUnorderedRows(expected_gsql_rows));
  }
}

TEST_P(SelectForUpdateTest, LimitOffsetOrderBy) {
  PopulateDatabase();
  auto txn = Transaction(Transaction::ReadWriteOptions());

  EXPECT_THAT(QueryTransaction(txn, R"sql(
      SELECT user_id, name
      FROM users
      ORDER BY user_id
      LIMIT 1
      FOR UPDATE)sql"),
              IsOkAndHoldsUnorderedRows({{1, "Douglas Adams"}}));

  EXPECT_THAT(QueryTransaction(txn, R"sql(
      SELECT user_id, name
      FROM users
      ORDER BY user_id
      LIMIT 1 OFFSET 1
      FOR UPDATE)sql"),
              IsOkAndHoldsUnorderedRows({{2, "Suzanne Collins"}}));

  EXPECT_THAT(QueryTransaction(txn, R"sql(
      SELECT user_id, name
      FROM users
      ORDER BY user_id DESC
      FOR UPDATE)sql"),
              IsOkAndHoldsUnorderedRows({{3, "J.R.R. Tolkien"},
                                         {2, "Suzanne Collins"},
                                         {1, "Douglas Adams"}}));
}

TEST_P(SelectForUpdateTest, GroupByHaving) {
  PopulateDatabase();
  auto txn = Transaction(Transaction::ReadWriteOptions());

  std::string sql = R"sql(
      SELECT user_id, thread_id, COUNT(message_id)
      FROM messages
      GROUP BY user_id, thread_id
      FOR UPDATE)sql";
  // In PG, GROUP BY clauses can't be combined with FOR UPDATE.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        QueryTransaction(txn, sql),
        StatusIs(testing::AnyOf(absl::StatusCode::kInvalidArgument,
                                absl::StatusCode::kUnimplemented),
                 testing::HasSubstr(
                     "FOR UPDATE is not allowed with GROUP BY clause")));
  } else {
    EXPECT_THAT(QueryTransaction(txn, sql),
                IsOkAndHoldsUnorderedRows({{1, 2, 1},
                                           {1, 4, 1},
                                           {2, 2, 1},
                                           {1, 1, 2},
                                           {1, 3, 1},
                                           {2, 1, 1},
                                           {3, 1, 1}}));
  }

  // In PG, HAVING clauses can't be combined with FOR UPDATE.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(QueryTransaction(txn,
                                 R"sql(
      SELECT user_id, thread_id, message_id
      FROM messages
      HAVING message_id > 1
      FOR UPDATE)sql"),
                StatusIs(testing::AnyOf(absl::StatusCode::kInvalidArgument,
                                        absl::StatusCode::kUnimplemented),
                         testing::HasSubstr(
                             "FOR UPDATE is not allowed with HAVING clause")));
  } else {
    EXPECT_THAT(QueryTransaction(txn,
                                 R"sql(
      SELECT user_id, thread_id, COUNT(message_id) AS cnt
      FROM messages
      GROUP BY user_id, thread_id
      HAVING cnt > 1
      FOR UPDATE)sql"),
                IsOkAndHoldsUnorderedRows({{1, 1, 2}}));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
