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

#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/access/read.h"
#include "backend/database/schema_cache.h"
#include "backend/datamodel/key_set.h"
#include "backend/query/query_engine.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "common/clock.h"
#include "common/config.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"

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

// Databases created from the same statements: the first builds the schema, the
// others copy it.
class DatabaseSchemaCacheTest : public DatabaseTest {
 protected:
  void SetUp() override {
    unsetenv("SPANNER_EMULATOR_DISABLE_SCHEMA_CACHE");
    SchemaCache::Instance().Clear();
    before_ = SchemaCache::Instance().stats();
  }

  void TearDown() override {
    unsetenv("SPANNER_EMULATOR_DISABLE_SCHEMA_CACHE");
    SchemaCache::Instance().Clear();
  }

  // What the cache did since the test began.
  SchemaCache::Stats Since() {
    SchemaCache::Stats now = SchemaCache::Instance().stats();
    return SchemaCache::Stats{
        .built = now.built - before_.built,
        .adopted = now.adopted - before_.adopted,
        .not_shareable = now.not_shareable - before_.not_shareable};
  }

  // A default, a generated column, an interleaved table, an index, a foreign
  // key, a check, a view, and a model, whose STRUCT output is a type that the
  // type factory of the database that builds the schema owns.
  std::vector<std::string> Statements() {
    return {
        "CREATE TABLE P(k INT64 NOT NULL, v STRING(MAX), "
        "tags ARRAY<STRING(MAX)>, scores ARRAY<FLOAT64>, "
        "d INT64 DEFAULT (7), g INT64 AS (k + 1) STORED) PRIMARY KEY(k)",
        "CREATE INDEX PByV ON P(v)",
        "CREATE TABLE C(k INT64 NOT NULL, c INT64 NOT NULL, "
        "CONSTRAINT positive CHECK (c > 0)) "
        "PRIMARY KEY(k, c), INTERLEAVE IN PARENT P ON DELETE CASCADE",
        "CREATE TABLE R(r INT64 NOT NULL, k INT64, "
        "CONSTRAINT fk FOREIGN KEY (k) REFERENCES P (k)) PRIMARY KEY(r)",
        "CREATE VIEW V SQL SECURITY INVOKER AS "
        "SELECT P.k, P.tags, P.g FROM P WHERE P.d = 7",
        "CREATE MODEL M INPUT(content STRING(MAX)) OUTPUT(embeddings "
        "STRUCT<statistics STRUCT<truncated BOOL, token_count FLOAT64>, "
        "values ARRAY<FLOAT64>>) REMOTE OPTIONS (endpoint = '//example/m')",
    };
  }

  std::unique_ptr<Database> Create(const std::string& id,
                                   const std::vector<std::string>& statements) {
    auto db = Database::Create(&clock_, id,
                               SchemaChangeOperation{.statements = statements});
    GOOGLESQL_EXPECT_OK(db.status());
    return db.ok() ? std::move(db).value() : nullptr;
  }

  void InsertP(Database* db, int64_t k, const std::string& v) {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadWriteTransaction> txn,
        db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
    Mutation m;
    m.AddWriteOp(
        MutationOpType::kInsert, "P", {"k", "v", "tags"},
        {{Int64(k), googlesql::values::String(v),
          googlesql::values::StringArray({"x", "y"})}});
    GOOGLESQL_ASSERT_OK(txn->Write(m));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  // The rows of `sql`, each as its values joined by commas.
  std::vector<std::string> Rows(Database* db, const std::string& sql) {
    std::vector<std::string> rows;
    auto txn = db->CreateReadOnlyTransaction(ReadOnlyOptions());
    GOOGLESQL_EXPECT_OK(txn.status());
    if (!txn.ok()) return rows;
    auto result = db->query_engine()->ExecuteSql(
        Query{sql}, QueryContext{(*txn)->schema(), txn->get()});
    GOOGLESQL_EXPECT_OK(result.status());
    if (!result.ok()) return rows;
    while (result->rows->Next()) {
      std::string row;
      for (int i = 0; i < result->rows->NumColumns(); ++i) {
        if (i > 0) row += ",";
        row += result->rows->ColumnValue(i).DebugString();
      }
      rows.push_back(row);
    }
    GOOGLESQL_EXPECT_OK(result->rows->Status());
    return rows;
  }

  // Reads the model's output, a STRUCT of a STRUCT and an ARRAY.
  static constexpr char kPredict[] =
      "SELECT embeddings.statistics.token_count, "
      "ARRAY_LENGTH(embeddings.values) "
      "FROM ML.PREDICT(MODEL M, (SELECT 'a' AS content))";

  absl::Status QueryStatus(Database* db, const std::string& sql) {
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<ReadOnlyTransaction> txn,
                               db->CreateReadOnlyTransaction(ReadOnlyOptions()));
    return db->query_engine()
        ->ExecuteSql(Query{sql}, QueryContext{txn->schema(), txn.get()})
        .status();
  }

  absl::Status Update(Database* db,
                      const std::vector<std::string>& statements) {
    absl::Status backfill_status;
    int completed_statements;
    absl::Time commit_ts;
    GOOGLESQL_RETURN_IF_ERROR(
        db->UpdateSchema(SchemaChangeOperation{.statements = statements},
                         &completed_statements, &commit_ts, &backfill_status));
    return backfill_status;
  }

  SchemaCache::Stats before_;
};

TEST_F(DatabaseSchemaCacheTest, SecondDatabaseCopiesTheSchemaOfTheFirst) {
  auto first = Create("first", Statements());
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(Since().built, 1);
  EXPECT_EQ(Since().adopted, 0);

  auto second = Create("second", Statements());
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 1);
  EXPECT_EQ(Since().adopted, 1);

  const Schema* a = first->GetLatestSchema();
  const Schema* b = second->GetLatestSchema();
  EXPECT_EQ(a->Dump().DebugString(), b->Dump().DebugString());
  EXPECT_EQ(b->Dump().statement_size(), 6);
  EXPECT_EQ(a->database_id(), "first");
  EXPECT_EQ(b->database_id(), "second");

  // The same schema, in nodes of its own, down to the storage IDs.
  ASSERT_EQ(a->GetSchemaGraph()->GetSchemaNodes().size(),
            b->GetSchemaGraph()->GetSchemaNodes().size());
  for (size_t i = 0; i < a->GetSchemaGraph()->GetSchemaNodes().size(); ++i) {
    const SchemaNode* in_a = a->GetSchemaGraph()->GetSchemaNodes()[i];
    const SchemaNode* in_b = b->GetSchemaGraph()->GetSchemaNodes()[i];
    EXPECT_NE(in_a, in_b);
    EXPECT_EQ(in_a->DebugString(), in_b->DebugString());
  }
  for (const Table* table : a->tables()) {
    const Table* other = b->FindTable(table->Name());
    ASSERT_NE(other, nullptr);
    EXPECT_NE(table, other);
    EXPECT_EQ(table->id(), other->id());
    ASSERT_EQ(table->columns().size(), other->columns().size());
    for (size_t i = 0; i < table->columns().size(); ++i) {
      EXPECT_NE(table->columns()[i], other->columns()[i]);
      EXPECT_EQ(table->columns()[i]->id(), other->columns()[i]->id());
      EXPECT_EQ(other->columns()[i]->table(), other);
      EXPECT_TRUE(table->columns()[i]->GetType()->Equals(
          other->columns()[i]->GetType()));
    }
  }
  EXPECT_EQ(b->FindIndex("PByV")->indexed_table(), b->FindTable("P"));
  EXPECT_EQ(b->FindTable("C")->parent(), b->FindTable("P"));
}

TEST_F(DatabaseSchemaCacheTest, RowsOfOneAreNotInTheOther) {
  auto first = Create("first", Statements());
  auto second = Create("second", Statements());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(Since().adopted, 1);

  InsertP(first.get(), 1, "one");
  InsertP(second.get(), 2, "two");
  InsertP(second.get(), 3, "three");

  EXPECT_THAT(Rows(first.get(), "SELECT k, v, d, g FROM P ORDER BY k"),
              testing::ElementsAre("1,\"one\",7,2"));
  EXPECT_THAT(Rows(second.get(), "SELECT k, v, d, g FROM P ORDER BY k"),
              testing::ElementsAre("2,\"two\",7,3", "3,\"three\",7,4"));
  EXPECT_THAT(Rows(first.get(), "SELECT k FROM P@{FORCE_INDEX=PByV}"),
              testing::ElementsAre("1"));
  EXPECT_THAT(Rows(second.get(), "SELECT COUNT(*) FROM V"),
              testing::ElementsAre("2"));
}

TEST_F(DatabaseSchemaCacheTest, SchemaChangeOnOneLeavesTheOtherAsItWas) {
  auto first = Create("first", Statements());
  auto second = Create("second", Statements());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(Since().adopted, 1);
  const std::string before = first->GetLatestSchema()->Dump().DebugString();

  InsertP(first.get(), 1, "one");
  InsertP(second.get(), 2, "two");
  GOOGLESQL_ASSERT_OK(Update(second.get(),
                             {"ALTER TABLE P ADD COLUMN extra INT64",
                              "ALTER TABLE P ALTER COLUMN v STRING(100)",
                              "DROP TABLE R", "DROP INDEX PByV",
                              "CREATE TABLE N(n INT64) PRIMARY KEY(n)",
                              "CREATE INDEX PByD ON P(d)"}));

  EXPECT_EQ(first->GetLatestSchema()->Dump().DebugString(), before);
  EXPECT_EQ(first->GetLatestSchema()->FindTable("P")->FindColumn("extra"),
            nullptr);
  EXPECT_NE(first->GetLatestSchema()->FindTable("R"), nullptr);
  EXPECT_NE(first->GetLatestSchema()->FindIndex("PByV"), nullptr);
  EXPECT_NE(second->GetLatestSchema()->FindTable("P")->FindColumn("extra"),
            nullptr);
  EXPECT_EQ(second->GetLatestSchema()->FindTable("R"), nullptr);

  EXPECT_THAT(Rows(first.get(), "SELECT k FROM P@{FORCE_INDEX=PByV}"),
              testing::ElementsAre("1"));
  EXPECT_FALSE(QueryStatus(first.get(), "SELECT extra FROM P").ok());
  GOOGLESQL_EXPECT_OK(QueryStatus(second.get(), "SELECT extra FROM P"));
  EXPECT_THAT(Rows(second.get(), "SELECT k, extra FROM P@{FORCE_INDEX=PByD}"),
              testing::ElementsAre("2,NULL"));

  // And the other way round: the builder changes, the copy stays.
  GOOGLESQL_ASSERT_OK(
      Update(first.get(), {"ALTER TABLE C ADD COLUMN note STRING(MAX)"}));
  EXPECT_EQ(second->GetLatestSchema()->FindTable("C")->FindColumn("note"),
            nullptr);

  // A third database still gets the schema as it was first built.
  auto third = Create("third", Statements());
  ASSERT_NE(third, nullptr);
  EXPECT_EQ(Since().adopted, 2);
  EXPECT_EQ(third->GetLatestSchema()->Dump().DebugString(), before);
}

// A database that copied a schema allocates the IDs of what it adds later
// where a build of its own would have left off.
TEST_F(DatabaseSchemaCacheTest, LaterTablesGetTheIdsABuildWouldHaveGiven) {
  auto first = Create("first", Statements());
  auto second = Create("second", Statements());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(Since().adopted, 1);

  const std::vector<std::string> more = {
      "CREATE TABLE N(n INT64, m INT64) PRIMARY KEY(n)",
      "CREATE INDEX NByM ON N(m)", "ALTER TABLE P ADD COLUMN extra INT64"};
  GOOGLESQL_ASSERT_OK(Update(first.get(), more));
  GOOGLESQL_ASSERT_OK(Update(second.get(), more));

  const Schema* a = first->GetLatestSchema();
  const Schema* b = second->GetLatestSchema();
  EXPECT_EQ(a->FindTable("N")->id(), b->FindTable("N")->id());
  EXPECT_EQ(a->FindTable("N")->FindColumn("m")->id(),
            b->FindTable("N")->FindColumn("m")->id());
  EXPECT_EQ(a->FindTable("P")->FindColumn("extra")->id(),
            b->FindTable("P")->FindColumn("extra")->id());
  EXPECT_EQ(a->FindIndex("NByM")->index_data_table()->id(),
            b->FindIndex("NByM")->index_data_table()->id());

  // No ID is handed out twice within the database.
  std::set<std::string> table_ids;
  std::set<std::string> column_ids;
  int tables = 0;
  int columns = 0;
  for (const SchemaNode* node : b->GetSchemaGraph()->GetSchemaNodes()) {
    if (const Table* table = node->As<const Table>(); table != nullptr) {
      table_ids.insert(table->id());
      ++tables;
    } else if (const Column* column = node->As<const Column>();
               column != nullptr) {
      column_ids.insert(column->id());
      ++columns;
    }
  }
  EXPECT_EQ(table_ids.size(), tables);
  EXPECT_EQ(column_ids.size(), columns);
}

TEST_F(DatabaseSchemaCacheTest, DroppingTheBuilderLeavesTheCopyUsable) {
  auto first = Create("first", Statements());
  auto second = Create("second", Statements());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(Since().adopted, 1);

  // Use the builder's type factory for more than the schema before it goes.
  InsertP(first.get(), 1, "one");
  EXPECT_THAT(
      Rows(first.get(), "SELECT ARRAY(SELECT AS STRUCT k, tags FROM P)"),
      testing::SizeIs(1));
  first.reset();
  // The cache lets go of the schema too: the copy alone keeps its types.
  SchemaCache::Instance().Clear();

  InsertP(second.get(), 2, "two");
  EXPECT_THAT(Rows(second.get(), "SELECT k, v, tags, scores, g FROM P"),
              testing::ElementsAre("2,\"two\",[\"x\", \"y\"],NULL,3"));
  EXPECT_THAT(Rows(second.get(), "SELECT k, tags FROM V"),
              testing::ElementsAre("2,[\"x\", \"y\"]"));
  EXPECT_THAT(Rows(second.get(),
                   "SELECT ARRAY(SELECT AS STRUCT k, tags, scores FROM P)"),
              testing::SizeIs(1));
  EXPECT_THAT(Rows(second.get(), kPredict), testing::SizeIs(1));
  EXPECT_THAT(Rows(second.get(),
                   "SELECT column_name, spanner_type "
                   "FROM information_schema.columns "
                   "WHERE table_name = 'P' AND column_name = 'tags'"),
              testing::ElementsAre("\"tags\",\"ARRAY<STRING(MAX)>\""));

  GOOGLESQL_ASSERT_OK(
      Update(second.get(),
             {"ALTER TABLE P ADD COLUMN more ARRAY<STRING(MAX)>",
              "CREATE TABLE N(n INT64, a ARRAY<FLOAT64>) PRIMARY KEY(n)",
              "CREATE INDEX PByG ON P(g)", "DROP TABLE R"}));
  InsertP(second.get(), 3, "three");
  EXPECT_THAT(
      Rows(second.get(),
           "SELECT k, tags, more FROM P@{FORCE_INDEX=PByG} ORDER BY k"),
      testing::ElementsAre("2,[\"x\", \"y\"],NULL", "3,[\"x\", \"y\"],NULL"));
  EXPECT_THAT(Rows(second.get(), kPredict), testing::SizeIs(1));

  // A database created now builds again, and is as usable.
  auto third = Create("third", Statements());
  ASSERT_NE(third, nullptr);
  EXPECT_EQ(Since().built, 2);
  second.reset();
  InsertP(third.get(), 4, "four");
  EXPECT_THAT(Rows(third.get(), "SELECT k, tags FROM V"),
              testing::ElementsAre("4,[\"x\", \"y\"]"));
}

TEST_F(DatabaseSchemaCacheTest, DroppingTheCopyLeavesTheBuilderUsable) {
  auto first = Create("first", Statements());
  auto second = Create("second", Statements());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  InsertP(second.get(), 2, "two");
  second.reset();
  SchemaCache::Instance().Clear();

  InsertP(first.get(), 1, "one");
  EXPECT_THAT(Rows(first.get(), "SELECT k, tags FROM V"),
              testing::ElementsAre("1,[\"x\", \"y\"]"));
  GOOGLESQL_ASSERT_OK(
      Update(first.get(), {"ALTER TABLE P ADD COLUMN extra INT64"}));
  EXPECT_THAT(Rows(first.get(), "SELECT k, extra FROM P"),
              testing::ElementsAre("1,NULL"));
  EXPECT_THAT(Rows(first.get(), kPredict), testing::SizeIs(1));
}

TEST_F(DatabaseSchemaCacheTest, ConcurrentCreatesBuildOnce) {
  constexpr int kDatabases = 6;
  const std::vector<std::string> statements = Statements();
  std::vector<std::unique_ptr<Database>> databases(kDatabases);
  std::vector<std::thread> threads;
  for (int i = 0; i < kDatabases; ++i) {
    threads.emplace_back([this, i, &statements, &databases] {
      auto db = Database::Create(
          &clock_, absl::StrCat("db", i),
          SchemaChangeOperation{.statements = statements});
      if (db.ok()) databases[i] = std::move(db).value();
    });
  }
  for (std::thread& thread : threads) thread.join();

  EXPECT_EQ(Since().built, 1);
  EXPECT_EQ(Since().adopted, kDatabases - 1);
  for (int i = 0; i < kDatabases; ++i) {
    ASSERT_NE(databases[i], nullptr);
    InsertP(databases[i].get(), i, "row");
  }
  for (int i = 0; i < kDatabases; ++i) {
    EXPECT_THAT(Rows(databases[i].get(), "SELECT k FROM P"),
                testing::ElementsAre(absl::StrCat(i)));
  }
}

TEST_F(DatabaseSchemaCacheTest, DifferentStatementsBuildTheirOwn) {
  std::vector<std::string> other = Statements();
  other.back() += " ";
  auto first = Create("first", Statements());
  auto second = Create("second", other);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 2);
  EXPECT_EQ(Since().adopted, 0);
}

TEST_F(DatabaseSchemaCacheTest, KeyCoversDialectFlagsProtosAndStatements) {
  const std::vector<std::string> statements = Statements();
  const std::string key =
      SchemaCache::KeyFor(SchemaChangeOperation{.statements = statements});
  EXPECT_EQ(key, SchemaCache::KeyFor(
                     SchemaChangeOperation{.statements = statements}));
  EXPECT_NE(key, SchemaCache::KeyFor(SchemaChangeOperation{
                     .statements = statements,
                     .database_dialect =
                         database_api::DatabaseDialect::POSTGRESQL}));
  EXPECT_NE(key, SchemaCache::KeyFor(SchemaChangeOperation{
                     .statements = statements,
                     .proto_descriptor_bytes = "descriptors"}));

  // Two statements are not one statement that reads the same end to end.
  const std::vector<std::string> split = {"CREATE TABLE A(a INT64) ",
                                          "PRIMARY KEY(a)"};
  const std::vector<std::string> joined = {
      "CREATE TABLE A(a INT64) PRIMARY KEY(a)"};
  EXPECT_NE(SchemaCache::KeyFor(SchemaChangeOperation{.statements = split}),
            SchemaCache::KeyFor(SchemaChangeOperation{.statements = joined}));

  EmulatorFeatureFlags::Flags flags = EmulatorFeatureFlags::instance().flags();
  flags.enable_views = !flags.enable_views;
  test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_NE(key, SchemaCache::KeyFor(
                     SchemaChangeOperation{.statements = statements}));
}

// A sequence counts outside its schema node, so a schema with one is built by
// every database that asks for it.
TEST_F(DatabaseSchemaCacheTest, SchemaWithASequenceIsNeverCopied) {
  const std::vector<std::string> statements = {
      "CREATE SEQUENCE S OPTIONS (sequence_kind = 'bit_reversed_positive')",
      "CREATE TABLE T(k INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE S)), "
      "v INT64) PRIMARY KEY(k)"};
  auto first = Create("first", statements);
  auto second = Create("second", statements);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 1);
  EXPECT_NE(second->GetLatestSchema()->FindSequence("S"), nullptr);
}

TEST_F(DatabaseSchemaCacheTest, SchemaWithAnIdentityColumnIsNeverCopied) {
  const std::vector<std::string> statements = {
      "CREATE TABLE T(k INT64 GENERATED BY DEFAULT AS IDENTITY "
      "(BIT_REVERSED_POSITIVE), v INT64) PRIMARY KEY(k)"};
  auto first = Create("first", statements);
  auto second = Create("second", statements);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 1);
}

TEST_F(DatabaseSchemaCacheTest, SchemaWithAChangeStreamIsNeverCopied) {
  const std::vector<std::string> statements = {
      "CREATE TABLE T(k INT64, v INT64) PRIMARY KEY(k)",
      "CREATE CHANGE STREAM Stream FOR ALL"};
  auto first = Create("first", statements);
  auto second = Create("second", statements);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 1);
  EXPECT_NE(second->GetLatestSchema()->FindChangeStream("Stream"), nullptr);
}

// A dropped table is recorded in the storage of the database that dropped it.
TEST_F(DatabaseSchemaCacheTest, SchemaBuiltWithADropIsNeverCopied) {
  const std::vector<std::string> statements = {
      "CREATE TABLE T(k INT64, v INT64) PRIMARY KEY(k)",
      "CREATE TABLE Gone(k INT64) PRIMARY KEY(k)", "DROP TABLE Gone"};
  auto first = Create("first", statements);
  auto second = Create("second", statements);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 1);
}

TEST_F(DatabaseSchemaCacheTest, PostgresqlDatabasesBuildTheirOwn) {
  const std::vector<std::string> statements = {
      "CREATE TABLE T(k1 bigint primary key, k2 bigint)"};
  for (int i = 0; i < 2; ++i) {
    GOOGLESQL_ASSERT_OK(Database::Create(
        &clock_, absl::StrCat("pg", i),
        SchemaChangeOperation{
            .statements = statements,
            .database_dialect = database_api::DatabaseDialect::POSTGRESQL}));
  }
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 0);
}

TEST_F(DatabaseSchemaCacheTest, FailedBuildIsTriedAgain) {
  const std::vector<std::string> statements = {
      "CREATE TABLE T(k INT64, v INT64) PRIMARY KEY(k)",
      "CREATE INDEX I ON T(missing)"};
  for (int i = 0; i < 2; ++i) {
    EXPECT_FALSE(Database::Create(
                     &clock_, "broken",
                     SchemaChangeOperation{.statements = statements})
                     .ok());
  }
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 0);
}

TEST_F(DatabaseSchemaCacheTest, DisabledByEnvironmentVariable) {
  setenv("SPANNER_EMULATOR_DISABLE_SCHEMA_CACHE", "1", /*overwrite=*/1);
  auto first = Create("first", Statements());
  auto second = Create("second", Statements());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(Since().built, 0);
  EXPECT_EQ(Since().adopted, 0);
  EXPECT_EQ(Since().not_shareable, 0);
  InsertP(first.get(), 1, "one");
  EXPECT_THAT(Rows(second.get(), "SELECT k FROM P"), testing::IsEmpty());

  // Back on, the next create builds and the one after copies.
  unsetenv("SPANNER_EMULATOR_DISABLE_SCHEMA_CACHE");
  auto third = Create("third", Statements());
  auto fourth = Create("fourth", Statements());
  EXPECT_EQ(Since().built, 1);
  EXPECT_EQ(Since().adopted, 1);
}

TEST_F(DatabaseSchemaCacheTest, KeepsOnlyTheLastFewSchemas) {
  auto statements_for = [](int i) {
    return std::vector<std::string>{absl::StrCat(
        "CREATE TABLE T", i, "(k INT64, v ARRAY<INT64>) PRIMARY KEY(k)")};
  };
  const int distinct = SchemaCache::kMaxEntries + 1;
  std::vector<std::unique_ptr<Database>> databases;
  for (int i = 0; i < distinct; ++i) {
    databases.push_back(Create(absl::StrCat("db", i), statements_for(i)));
  }
  EXPECT_EQ(Since().built, distinct);

  // The newest is still there, the oldest was pushed out and builds again.
  databases.push_back(Create("newest", statements_for(distinct - 1)));
  EXPECT_EQ(Since().adopted, 1);
  EXPECT_EQ(Since().built, distinct);
  databases.push_back(Create("oldest", statements_for(0)));
  EXPECT_EQ(Since().adopted, 1);
  EXPECT_EQ(Since().built, distinct + 1);
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
