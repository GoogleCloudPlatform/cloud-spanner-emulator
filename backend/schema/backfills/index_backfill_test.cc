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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/types/span.h"
#include "backend/database/database.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/options.h"
#include "common/errors.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"
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

// TODO: This is just a temporary test to verify basic
// functionality until the schema constructor has been finished. Replace with
// more robust testing after schema constructor has been finished.
class BackfillTest : public ::testing::Test {
 public:
  absl::Status UpdateSchema(absl::Span<const std::string> update_statements) {
    int num_succesful;
    absl::Status backfill_status;
    absl::Time update_time;
    ZETASQL_RETURN_IF_ERROR(database_->UpdateSchema(update_statements, &num_succesful,
                                            &update_time, &backfill_status));
    return backfill_status;
  }

 protected:
  void SetUp() override {
    std::vector<std::string> create_statements;
    create_statements.push_back(R"(
                            CREATE TABLE TestTable (
                              int64_col INT64,
                              string_col STRING(MAX),
                              another_string_col STRING(MAX)
                            ) PRIMARY KEY (int64_col)
                          )");
    ZETASQL_ASSERT_OK_AND_ASSIGN(database_,
                         Database::Create(&clock_, create_statements));

    index_update_statements_.push_back(R"(
                            CREATE UNIQUE NULL_FILTERED INDEX TestIndex ON
                            TestTable(string_col DESC)
                            STORING(another_string_col)
                    )");
  }

  // Test components.
  Clock clock_;
  std::unique_ptr<Database> database_;
  const Schema* schema_;

  std::vector<std::string> index_update_statements_;
};

TEST_F(BackfillTest, BackfillIndex) {
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    // Check that table exists and index doesn't.
    schema_ = txn->schema();
    EXPECT_THAT(schema_->tables().size(), 1);
    EXPECT_THAT(schema_->tables()[0]->indexes().size(), 0);
    EXPECT_THAT(schema_->tables()[0]->Name(), "TestTable");

    // Buffer mutations.
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_col"}, {{Int64(1), String("value1")}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_col", "another_string_col"},
                 {{Int64(2), String("value2"), String("test")}});
    ZETASQL_EXPECT_OK(txn->Write(m));
    ZETASQL_EXPECT_OK(txn->Commit());
  }

  // Verify current values in table.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));

    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.table = "TestTable";
    read_arg.columns = {"int64_col", "string_col"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
      values.push_back(cursor->ColumnValue(1));
    }
    EXPECT_THAT(values, testing::ElementsAre(Int64(1), String("value1"),
                                             Int64(2), String("value2")));
  }

  // Update the schema to include an index.
  ZETASQL_EXPECT_OK(UpdateSchema(index_update_statements_));
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));

    // Check that index now exists.
    schema_ = txn->schema();
    EXPECT_THAT(schema_->tables().size(), 1);
    EXPECT_THAT(schema_->tables()[0]->indexes().size(), 1);
    EXPECT_THAT(schema_->tables()[0]->Name(), "TestTable");
    EXPECT_THAT(schema_->tables()[0]->indexes()[0]->Name(), "TestIndex");
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));
    // Verify the values from the index.
    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.table = "TestTable";
    read_arg.index = "TestIndex";
    read_arg.columns = {"string_col", "int64_col", "another_string_col"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
      values.push_back(cursor->ColumnValue(1));
      values.push_back(cursor->ColumnValue(2));
    }
    EXPECT_THAT(values,
                testing::ElementsAre(String("value2"), Int64(2), String("test"),
                                     String("value1"), Int64(1), NullString()));
  }
}

TEST_F(BackfillTest, BackfillIndexWithNulls) {
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    // Buffer mutations.
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col"},
                 {{Int64(1)}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col"},
                 {{Int64(2)}});
    ZETASQL_EXPECT_OK(txn->Write(m));
    ZETASQL_EXPECT_OK(txn->Commit());
  }

  // Verify current values in table.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));

    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.table = "TestTable";
    read_arg.columns = {"int64_col", "string_col"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
      values.push_back(cursor->ColumnValue(1));
    }
    EXPECT_THAT(values, testing::ElementsAre(Int64(1), NullString(), Int64(2),
                                             NullString()));
  }

  // Update the schema to include an index.
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
      CREATE INDEX TestIndex ON TestTable(string_col DESC)
  )"}));

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));

    // Check that index now exists.
    schema_ = txn->schema();
    EXPECT_THAT(schema_->tables().size(), 1);
    EXPECT_THAT(schema_->tables()[0]->indexes().size(), 1);
    EXPECT_THAT(schema_->tables()[0]->Name(), "TestTable");
    EXPECT_THAT(schema_->tables()[0]->indexes()[0]->Name(), "TestIndex");
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));
    // Verify the values from the index.
    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.table = "TestTable";
    read_arg.index = "TestIndex";
    read_arg.columns = {"string_col", "int64_col"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
      values.push_back(cursor->ColumnValue(1));
    }
    EXPECT_THAT(values, testing::ElementsAre(NullString(), Int64(1),
                                             NullString(), Int64(2)));
  }
}

TEST_F(BackfillTest, BackfillUniqueIndex) {
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    // Check that table exists and index doesn't.
    schema_ = txn->schema();
    EXPECT_THAT(schema_->tables().size(), 1);
    EXPECT_THAT(schema_->tables()[0]->indexes().size(), 0);
    EXPECT_THAT(schema_->tables()[0]->Name(), "TestTable");

    // Buffer mutations.
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_col"}, {{Int64(1), String("value")}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_col"}, {{Int64(2), String("value")}});
    ZETASQL_EXPECT_OK(txn->Write(m));
    ZETASQL_EXPECT_OK(txn->Commit());
  }

  // Verify current values in table.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));

    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.table = "TestTable";
    read_arg.columns = {"int64_col", "string_col"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
      values.push_back(cursor->ColumnValue(1));
    }
    EXPECT_THAT(values, testing::ElementsAre(Int64(1), String("value"),
                                             Int64(2), String("value")));
  }

  // Update the schema to include a unique index, which should fail on backfill.
  EXPECT_EQ(UpdateSchema(index_update_statements_),
            error::UniqueIndexViolationOnIndexCreation(
                "TestIndex", R"({String("value")↓})"));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
