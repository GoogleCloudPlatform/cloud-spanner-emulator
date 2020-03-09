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

}  // namespace

class ReadYourWritesTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE CommitTsTable (
            id STRING(MAX) NOT NULL,
            ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
            val STRING(MAX)
          ) PRIMARY KEY (id, ts)
        )",
        R"(
          CREATE INDEX CommitTsTableIndex ON CommitTsTable(val)
        )",
        R"(
          CREATE INDEX CommitTsTableUniqueIndex ON CommitTsTable(val)
        )",
        R"(
          CREATE TABLE NonKeyCommitTsTable (
            id STRING(MAX) NOT NULL,
            key STRING(MAX) NOT NULL,
            ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          ) PRIMARY KEY (id, key)
        )",
        R"(
          CREATE INDEX NonKeyCommitTsIndexOnTs ON NonKeyCommitTsTable(ts)
        )",
        R"(
          CREATE INDEX NonKeyCommitTsIndexOnKey ON NonKeyCommitTsTable(key)
        )"}));
    return zetasql_base::OkStatus();
  }
};

TEST_F(ReadYourWritesTest, CanReadAlreadyCommittedTimestampKey) {
  // Reading a commit_timestamp key that doesn't include buffered mutations
  // works.
  ZETASQL_ASSERT_OK_AND_ASSIGN(CommitResult result,
                       Insert("CommitTsTable", {"id", "ts", "val"},
                              {"id1", kCommitTimestampSentinel, "val1"}));
  EXPECT_THAT(Query("SELECT id, ts, val FROM CommitTsTable"),
              IsOkAndHoldsRows({{"id1", result.commit_timestamp, "val1"}}));
}

TEST_F(ReadYourWritesTest, CanReadExplictlySetTimestampKey) {
  // Perform writes and reads using the same transaction.
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Reading an explicitly set commit timestamp in buffered mutation works.
  auto explicit_ts = MakeNowTimestamp();
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn, SqlStatement("INSERT INTO CommitTsTable (id, ts, val) "
                        "VALUES (@id, @ts, @val)",
                        {{"id", Value("id1")},
                         {"ts", Value(explicit_ts)},
                         {"val", Value("val1")}})));
  EXPECT_THAT(QueryTransaction(txn, "SELECT id, ts, val FROM CommitTsTable"),
              IsOkAndHoldsRows({{"id1", explicit_ts, "val1"}}));
}

TEST_F(ReadYourWritesTest, CannotReadPendingCommitTimestampInKey) {
  // Perform writes and reads using the same transaction.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn, SqlStatement("INSERT INTO CommitTsTable (id, ts, val) "
                        "VALUES (@id, PENDING_COMMIT_TIMESTAMP(), @val)",
                        {{"id", Value("id1")}, {"val", Value("val1")}})));

  // Reading a pending commit_timestamp is not supported.
  EXPECT_THAT(QueryTransaction(txn, "SELECT ts FROM CommitTsTable"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Trying to read the non-timestamp key column is not supported.
  EXPECT_THAT(QueryTransaction(txn, "SELECT id FROM CommitTsTable"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Trying to read the non-key column val is also not supported.
  EXPECT_THAT(QueryTransaction(txn, "SELECT val FROM CommitTsTable"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Cannot read non-key values using index since primary key of table is part
  // of index implicitly.
  EXPECT_THAT(
      QueryTransaction(
          txn,
          "SELECT val FROM CommitTsTable@{force_index=CommitTsTableIndex}"),
      StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(
      QueryTransaction(txn,
                       "SELECT val FROM "
                       "CommitTsTable@{force_index=CommitTsTableUniqueIndex}"),
      StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(ReadYourWritesTest, CanReadAlreadyCommittedTimestampColumn) {
  // Reading a commit_timestamp column that doesn't include buffered mutations
  // works.
  ZETASQL_ASSERT_OK_AND_ASSIGN(CommitResult result,
                       Insert("NonKeyCommitTsTable", {"id", "key", "ts"},
                              {"id1", "key1", kCommitTimestampSentinel}));
  EXPECT_THAT(Query("SELECT id, key, ts FROM NonKeyCommitTsTable"),
              IsOkAndHoldsRows({{"id1", "key1", result.commit_timestamp}}));
}

TEST_F(ReadYourWritesTest, CanReadExplictlySetTimestampColumn) {
  // Perform writes and reads using the same transaction.
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Reading an explicitly set commit timestamp in buffered mutation works.
  auto explicit_ts = MakeNowTimestamp();
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn, SqlStatement("INSERT INTO NonKeyCommitTsTable (id, key, ts) "
                        "VALUES (@id, @key, @ts)",
                        {{"id", Value("id1")},
                         {"key", Value("key1")},
                         {"ts", Value(explicit_ts)}})));
  EXPECT_THAT(
      QueryTransaction(txn, "SELECT id, key, ts FROM NonKeyCommitTsTable"),
      IsOkAndHoldsRows({{"id1", "key1", explicit_ts}}));
}

TEST_F(ReadYourWritesTest, CannotReadPendingCommitTimestampInColumn) {
  // Perform writes and reads using the same transaction.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn, SqlStatement("INSERT INTO NonKeyCommitTsTable (id, key, ts) "
                        "VALUES (@id, @key, PENDING_COMMIT_TIMESTAMP())",
                        {{"id", Value("id1")}, {"key", Value("key1")}})));

  // Reading a pending commit_timestamp column is not supported.
  EXPECT_THAT(QueryTransaction(txn, "SELECT ts FROM NonKeyCommitTsTable"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Cannot read pending commit-timestamp column using index.
  EXPECT_THAT(QueryTransaction(
                  txn,
                  "SELECT ts FROM "
                  "NonKeyCommitTsTable@{force_index=NonKeyCommitTsIndexOnTs}"),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// TODO: These tests below fail since Sql query will always try to
// read all the columns, and not just the columns requested by the end user. Fix
// sql query and enable the tests below.
TEST_F(ReadYourWritesTest,
       DISABLED_CanReadColumnsWithPendingCommitTimestampInNonKeyColumn) {
  // Perform writes and reads using the same transaction.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn, SqlStatement("INSERT INTO NonKeyCommitTsTable (id, key, ts) "
                        "VALUES (@id, @key, PENDING_COMMIT_TIMESTAMP())",
                        {{"id", Value("id1")}, {"key", Value("key1")}})));

  // Trying to read other non-timestamp key columns is okay.
  EXPECT_THAT(QueryTransaction(txn, "SELECT id, key FROM NonKeyCommitTsTable"),
              IsOkAndHoldsRows({{"id1", "key1"}}));

  // Can read other non-commit-timestamp columns using index though.
  EXPECT_THAT(QueryTransaction(
                  txn,
                  "SELECT key FROM "
                  "NonKeyCommitTsTable@{force_index=NonKeyCommitTsIndexOnKey}"),
              IsOkAndHoldsRows({{"key1"}}));
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
