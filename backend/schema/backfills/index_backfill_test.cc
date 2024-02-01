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

#include "backend/schema/backfills/index_backfill.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/database/database.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::NullString;
using zetasql::values::String;
using testing::ContainsRegex;
using testing::ElementsAre;
using testing::ElementsAreArray;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

struct DbInfo {
  std::unique_ptr<Clock> clock;
  std::unique_ptr<Database> database;
};

constexpr char kCreateTestTable[] = R"(
                            CREATE TABLE TestTable (
                              int64_col INT64,
                              string_col STRING(MAX),
                              another_string_col STRING(MAX),
                              extra_string_col STRING(MAX)
                            ) PRIMARY KEY (int64_col)
                          )";

absl::StatusOr<DbInfo> CreateTestDb(
    const absl::Span<const std::string> create_statements) {
  DbInfo db_info = {.clock = std::make_unique<Clock>()};
  ZETASQL_ASSIGN_OR_RETURN(
      db_info.database,
      Database::Create(db_info.clock.get(),
                       SchemaChangeOperation{.statements = create_statements}));
  return db_info;
}

absl::Status UpdateSchema(
    const DbInfo& db_info,
    const absl::Span<const std::string> update_statements) {
  int num_succesful;
  absl::Status backfill_status;
  absl::Time update_time;
  ZETASQL_RETURN_IF_ERROR(db_info.database->UpdateSchema(
      SchemaChangeOperation{.statements = update_statements}, &num_succesful,
      &update_time, &backfill_status));
  return backfill_status;
}

absl::StatusOr<std::vector<ValueList>> ReadAllRows(const DbInfo& db_info,
                                                   ReadArg read_arg) {
  read_arg.key_set = KeySet::All();
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ReadOnlyTransaction> txn,
      db_info.database->CreateReadOnlyTransaction(ReadOnlyOptions()));
  std::vector<ValueList> values;
  std::unique_ptr<RowCursor> cursor;
  ZETASQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));
  while (cursor->Next()) {
    ValueList row_value;
    row_value.reserve(read_arg.columns.size());
    for (int i = 0; i < read_arg.columns.size(); i++) {
      row_value.push_back(cursor->ColumnValue(i));
    }
    values.push_back(std::move(row_value));
  }
  return values;
}

absl::Status InsertValues(DbInfo& db_info, std::string table,
                          std::vector<std::string> columns,
                          std::vector<ValueList> values) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ReadWriteTransaction> txn,
                   db_info.database->CreateReadWriteTransaction(
                       ReadWriteOptions(), RetryState()));

  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, table, columns, values);
  ZETASQL_EXPECT_OK(txn->Write(m));
  ZETASQL_EXPECT_OK(txn->Commit());
  return absl::OkStatus();
}

TEST(BackfillTest, SchemaChange) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(UpdateSchema(db_info, {R"(
                            CREATE INDEX TestIndex ON
                            TestTable(string_col DESC)
                            STORING(another_string_col)
                    )"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                       db_info.database->CreateReadWriteTransaction(
                           ReadWriteOptions(), RetryState()));

  const Schema* schema = txn->schema();
  EXPECT_EQ(schema->tables().size(), 1);
  EXPECT_EQ(schema->tables()[0]->Name(), "TestTable");
  EXPECT_EQ(schema->tables()[0]->indexes().size(), 1);
  EXPECT_EQ(schema->tables()[0]->Name(), "TestTable");
  EXPECT_EQ(schema->tables()[0]->indexes()[0]->Name(), "TestIndex");
  EXPECT_EQ(schema->tables()[0]->indexes()[0]->stored_columns().size(), 1);
}

TEST(BackfillTest, BasicTableValues) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  std::string table = "TestTable";
  std::vector<std::string> columns = {"int64_col", "string_col",
                                      "another_string_col", "extra_string_col"};
  std::vector<ValueList> values_write = {
      {Int64(1), String("value1"), NullString(), String("extra")},
      {Int64(2), String("value2"), String("test"), NullString()}};
  ZETASQL_ASSERT_OK(InsertValues(db_info, table, columns, values_write));

  EXPECT_THAT(ReadAllRows(db_info, ReadArg{.table = table, .columns = columns}),
              IsOkAndHolds(ElementsAreArray(values_write)));
}

TEST(BackfillTest, BasicIndexValues) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(InsertValues(
      db_info, "TestTable",
      {"int64_col", "string_col", "another_string_col", "extra_string_col"},
      {{Int64(1), String("value1"), NullString(), String("extra")},
       {Int64(2), String("value2"), String("test"), NullString()}}));

  ZETASQL_ASSERT_OK(UpdateSchema(db_info, {R"(
                            CREATE INDEX TestIndex ON
                            TestTable(string_col DESC)
                            STORING(another_string_col)
                    )"}));

  EXPECT_THAT(
      ReadAllRows(db_info, ReadArg{.table = "TestTable",
                                   .index = "TestIndex",
                                   .columns = {"string_col", "int64_col",
                                               "another_string_col"}}),
      IsOkAndHolds(
          ElementsAre(ValueList{String("value2"), Int64(2), String("test")},
                      ValueList{String("value1"), Int64(1), NullString()})));
}

TEST(BackfillTest, IndexNullValues) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(InsertValues(db_info, "TestTable", {"int64_col"},
                         {{Int64(1)}, {Int64(2)}}));

  ZETASQL_ASSERT_OK(UpdateSchema(db_info, {R"(
                            CREATE INDEX TestIndex ON
                            TestTable(string_col DESC)
                    )"}));

  EXPECT_THAT(
      ReadAllRows(db_info, ReadArg{.table = "TestTable",
                                   .index = "TestIndex",
                                   .columns = {"string_col", "int64_col"}}),
      IsOkAndHolds(ElementsAre(ValueList{NullString(), Int64(1)},
                               ValueList{NullString(), Int64(2)})));
}

TEST(BackfillTest, NullFilteredIndexValues) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(InsertValues(
      db_info, "TestTable",
      {"int64_col", "string_col", "another_string_col", "extra_string_col"},
      {{Int64(1), String("value1"), NullString(), String("extra")},
       {Int64(2), String("value2"), String("test"), NullString()}}));

  ZETASQL_ASSERT_OK(UpdateSchema(db_info, {R"(
                            CREATE NULL_FILTERED INDEX TestIndex ON
                            TestTable(another_string_col DESC)
                            STORING(extra_string_col)
                    )"}));

  EXPECT_THAT(ReadAllRows(db_info,
                          ReadArg{.table = "TestTable",
                                  .index = "TestIndex",
                                  .columns = {"another_string_col", "int64_col",
                                              "extra_string_col"}}),
              IsOkAndHolds(ElementsAre(
                  ValueList{String("test"), Int64(2), NullString()})));
}

TEST(BackfillTest, BackfillUniqueIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(
      InsertValues(db_info, "TestTable", {"int64_col", "string_col"},
                   {{Int64(1), String("value")}, {Int64(2), String("value")}}));

  EXPECT_EQ(UpdateSchema(db_info, {R"(
                            CREATE UNIQUE INDEX TestIndex ON
                            TestTable(string_col DESC)
                    )"}),
            error::UniqueIndexViolationOnIndexCreation(
                "TestIndex", R"({String("value")↓})"));
}

TEST(BackfillTest, AlterIndexDropColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(InsertValues(
      db_info, "TestTable",
      {"int64_col", "string_col", "another_string_col", "extra_string_col"},
      {{Int64(1), String("value1"), NullString(), String("extra")},
       {Int64(2), String("value2"), String("test"), NullString()}}));

  ZETASQL_ASSERT_OK(UpdateSchema(db_info, {R"(
                            CREATE INDEX TestIndex ON
                            TestTable(string_col DESC)
                            STORING(another_string_col, extra_string_col)
                    )"}));

  ZETASQL_ASSERT_OK(UpdateSchema(
      db_info,
      {R"(ALTER INDEX TestIndex DROP STORED COLUMN another_string_col)"}));

  EXPECT_THAT(ReadAllRows(db_info, ReadArg{.table = "TestTable",
                                           .index = "TestIndex",
                                           .columns = {"another_string_col"}}),
              StatusIs(absl::StatusCode::kNotFound,
                       ContainsRegex(
                           "does not have a column named another_string_col")));

  EXPECT_THAT(
      ReadAllRows(db_info, ReadArg{.table = "TestTable",
                                   .index = "TestIndex",
                                   .columns = {"string_col", "int64_col",
                                               "extra_string_col"}}),
      IsOkAndHolds(
          ElementsAre(ValueList{String("value2"), Int64(2), NullString()},
                      ValueList{String("value1"), Int64(1), String("extra")})));
}

TEST(BackfillTest, AlterIndexAddColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DbInfo db_info, CreateTestDb({kCreateTestTable}));
  ZETASQL_ASSERT_OK(InsertValues(
      db_info, "TestTable",
      {"int64_col", "string_col", "another_string_col", "extra_string_col"},
      {{Int64(1), String("value1"), NullString(), String("extra")},
       {Int64(2), String("value2"), String("test"), NullString()}}));

  ZETASQL_ASSERT_OK(UpdateSchema(db_info, {R"(
                            CREATE INDEX TestIndex ON
                            TestTable(string_col DESC)
                            STORING(another_string_col)
                    )"}));
  ZETASQL_ASSERT_OK(UpdateSchema(
      db_info, {"ALTER INDEX TestIndex ADD STORED COLUMN extra_string_col"}));

  EXPECT_THAT(
      ReadAllRows(db_info, ReadArg{.table = "TestTable",
                                   .index = "TestIndex",
                                   .columns = {"string_col", "int64_col",
                                               "another_string_col",
                                               "extra_string_col"}}),
      IsOkAndHolds(ElementsAre(
          ValueList{String("value2"), Int64(2), String("test"), NullString()},
          ValueList{String("value1"), Int64(1), NullString(),
                    String("extra")})));
}
}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
