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

#include "backend/database/database.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/access/read.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "common/clock.h"
#include "common/config.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using googlesql::values::Int64;
using googlesql_base::testing::StatusIs;

constexpr char kDatabaseId[] = "test-db";

class DatabaseTest : public ::testing::Test {
 public:
  DatabaseTest() = default;

  ReadArg read_column(std::string table_name, std::string column_name) {
    ReadArg args;
    args.table = table_name;
    args.key_set = KeySet::All();
    args.columns = std::vector<std::string>{column_name};
    return args;
  }

 protected:
  Clock clock_;
};

TEST_F(DatabaseTest, CreateSuccessful) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Database> database,
      Database::Create(&clock_, kDatabaseId, SchemaChangeOperation{}));
  // Verifies that by default, a GoogleSQL database is created.
  EXPECT_EQ(database->dialect(),
            database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )",
                                                R"(
    CREATE INDEX I on T(k1))"};

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      database,
      Database::Create(&clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));
  // Verifies that by default, a GoogleSQL database is created.
  EXPECT_EQ(database->dialect(),
            database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);
}

TEST_F(DatabaseTest, CreateWithGSQLDialectSuccessful) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Database> database,
      Database::Create(
          &clock_, kDatabaseId,
          SchemaChangeOperation{
              .database_dialect =
                  database_api::DatabaseDialect::GOOGLE_STANDARD_SQL}));
  EXPECT_EQ(database->dialect(),
            database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )",
                                                R"(
    CREATE INDEX I on T(k1))"};

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      database,
      Database::Create(
          &clock_, kDatabaseId,
          SchemaChangeOperation{
              .statements = create_statements,
              .database_dialect =
                  database_api::DatabaseDialect::GOOGLE_STANDARD_SQL}));
  EXPECT_EQ(database->dialect(),
            database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);
}

TEST_F(DatabaseTest, CreateWithPostgresDialectSuccessful) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Database> database,
      Database::Create(
          &clock_, kDatabaseId,
          SchemaChangeOperation{
              .database_dialect = database_api::DatabaseDialect::POSTGRESQL}));
  EXPECT_EQ(database->dialect(), database_api::DatabaseDialect::POSTGRESQL);

  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 bigint primary key,
      k2 bigint
    )
  )",
                                                R"(
    CREATE INDEX I on T(k1))"};

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      database,
      Database::Create(
          &clock_, kDatabaseId,
          SchemaChangeOperation{
              .statements = create_statements,
              .database_dialect = database_api::DatabaseDialect::POSTGRESQL}));
  EXPECT_EQ(database->dialect(), database_api::DatabaseDialect::POSTGRESQL);
}

TEST_F(DatabaseTest, UpdateSchemaSuccessful) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db, Database::Create(&clock_, kDatabaseId, SchemaChangeOperation{}));

  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )",
                                                R"(
    CREATE INDEX I on T(k1)
  )"};

  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  GOOGLESQL_EXPECT_OK(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status));
  GOOGLESQL_EXPECT_OK(backfill_status);
}

TEST_F(DatabaseTest, UpdateSchemaPartialSuccess) {
  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db,
      Database::Create(&clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));

  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(1), Int64(2)}});
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(2), Int64(2)}});
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(3), Int64(2)}});
  GOOGLESQL_ASSERT_OK(txn->Write(m));
  GOOGLESQL_ASSERT_OK(txn->Commit());

  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T1(
      a INT64,
    ) PRIMARY KEY(a)
  )",
                                                R"(
    CREATE UNIQUE INDEX Idx on T(k2)
  )",
                                                R"(
    CREATE TABLE T2(
      b INT64,
    ) PRIMARY KEY(b)
  )"};

  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;

  // The statements are semantically valid, indicated by an OK return status.
  GOOGLESQL_EXPECT_OK(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status));

  // But the backfill statements fail.
  EXPECT_EQ(backfill_status,
            error::UniqueIndexViolationOnIndexCreation("Idx", "{Int64(2)}"));

  // Only the first statement in the batch is successfuly applied.
  EXPECT_EQ(completed_statements, 1);
}

// A schema snapshot is released during the change once no backfill can read
// it. The one a failed backfill falls back to is not among those.
TEST_F(DatabaseTest, UpdateSchemaFallsBackToSnapshotBeforeFailedBackfill) {
  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db,
      Database::Create(&clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(1), Int64(2)}});
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(2), Int64(2)}});
  GOOGLESQL_ASSERT_OK(txn->Write(m));
  GOOGLESQL_ASSERT_OK(txn->Commit());

  // Four statements with nothing to verify or backfill, then an index whose
  // backfill fails, then two more statements.
  std::vector<std::string> update_statements = {
      "CREATE TABLE A1(a INT64) PRIMARY KEY(a)",
      "CREATE TABLE A2(a INT64) PRIMARY KEY(a)",
      "CREATE TABLE A3(a INT64) PRIMARY KEY(a)",
      "CREATE TABLE A4(a INT64) PRIMARY KEY(a)",
      "CREATE UNIQUE INDEX Idx on T(k2)",
      "CREATE TABLE A5(a INT64) PRIMARY KEY(a)",
      "CREATE TABLE A6(a INT64) PRIMARY KEY(a)",
  };

  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  GOOGLESQL_EXPECT_OK(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status));
  EXPECT_EQ(backfill_status,
            error::UniqueIndexViolationOnIndexCreation("Idx", "{Int64(2)}"));
  EXPECT_EQ(completed_statements, 4);

  const Schema* schema = db->GetLatestSchema();
  EXPECT_NE(schema->FindTable("A1"), nullptr);
  EXPECT_NE(schema->FindTable("A4"), nullptr);
  EXPECT_EQ(schema->FindIndex("Idx"), nullptr);
  EXPECT_EQ(schema->FindTable("A5"), nullptr);

  // The schema it fell back to takes further changes and reads.
  std::vector<std::string> more_statements = {
      "CREATE INDEX Idx2 on T(k2)",
      "CREATE TABLE A5(a INT64) PRIMARY KEY(a)",
  };
  GOOGLESQL_EXPECT_OK(
      db->UpdateSchema(SchemaChangeOperation{.statements = more_statements},
                       &completed_statements, &commit_ts, &backfill_status));
  GOOGLESQL_EXPECT_OK(backfill_status);
  EXPECT_EQ(completed_statements, 2);
  EXPECT_NE(db->GetLatestSchema()->FindIndex("Idx2"), nullptr);
}

// A database is created by running each statement's backfills as soon as the
// statement is applied. The schema it ends with is that of all the statements.
TEST_F(DatabaseTest, CreateRunsEveryStatementWithItsBackfills) {
  std::vector<std::string> create_statements = {
      "CREATE TABLE P(k INT64, v STRING(MAX), d INT64 DEFAULT (7), "
      "g INT64 AS (k + 1) STORED) PRIMARY KEY(k)",
      "CREATE INDEX PByV ON P(v)",
      "CREATE TABLE C(k INT64, c INT64, CONSTRAINT positive CHECK (c > 0)) "
      "PRIMARY KEY(k, c), INTERLEAVE IN PARENT P ON DELETE CASCADE",
      "ALTER TABLE C ADD CONSTRAINT fk FOREIGN KEY (k) REFERENCES P (k)",
      "CREATE TABLE Unused(k INT64) PRIMARY KEY(k)",
      "DROP TABLE Unused",
      "CREATE UNIQUE NULL_FILTERED INDEX CByC ON C(c)",
      "ALTER TABLE P ADD COLUMN w BOOL",
  };
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db,
      Database::Create(&clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));

  const Schema* schema = db->GetLatestSchema();
  ASSERT_NE(schema->FindTable("P"), nullptr);
  EXPECT_NE(schema->FindTable("P")->FindColumn("w"), nullptr);
  EXPECT_NE(schema->FindTable("C"), nullptr);
  EXPECT_EQ(schema->FindTable("Unused"), nullptr);
  EXPECT_NE(schema->FindIndex("PByV"), nullptr);
  EXPECT_NE(schema->FindIndex("CByC"), nullptr);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "P", {"k", "v"},
               {{Int64(1), googlesql::values::String("a")}});
  m.AddWriteOp(MutationOpType::kInsert, "C", {"k", "c"},
               {{Int64(1), Int64(5)}});
  GOOGLESQL_ASSERT_OK(txn->Write(m));
  GOOGLESQL_ASSERT_OK(txn->Commit());
}

// An invalid statement fails the create whatever came before it.
TEST_F(DatabaseTest, CreateFailsOnALaterInvalidStatement) {
  std::vector<std::string> create_statements = {
      "CREATE TABLE P(k INT64, v STRING(MAX)) PRIMARY KEY(k)",
      "CREATE INDEX PByV ON P(v)",
      "CREATE INDEX PByMissing ON P(missing)",
  };
  EXPECT_FALSE(Database::Create(&clock_, kDatabaseId,
                                SchemaChangeOperation{
                                    .statements = create_statements})
                   .ok());
}

TEST_F(DatabaseTest, ConcurrentSchemaChangeIsAborted) {
  auto current_probability = config::abort_current_transaction_probability();
  config::set_abort_current_transaction_probability(0);

  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db,
      Database::Create(&clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));

  // Initiate a Read inside a read-write transaction to acquire locks.
  std::unique_ptr<RowCursor> row_cursor;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  GOOGLESQL_EXPECT_OK(txn->Read(read_column("T", "k1"), &row_cursor));

  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  EXPECT_EQ(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status),
      error::ConcurrentSchemaChangeOrReadWriteTxnInProgress());

  config::set_abort_current_transaction_probability(current_probability);
}

TEST_F(DatabaseTest, SchemaChangeLocksSuccesfullyReleased) {
  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db,
      Database::Create(&clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));

  // Schema update will fail.
  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  EXPECT_FALSE(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status)
          .ok());

  // Can still run transactions as locks would have been released.
  std::unique_ptr<RowCursor> row_cursor;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  GOOGLESQL_EXPECT_OK(txn->Read(read_column("T", "k1"), &row_cursor));
  GOOGLESQL_EXPECT_OK(txn->Commit());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
