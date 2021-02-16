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

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class LimitsTest : public DatabaseTest {
  absl::Status SetUpDatabase() override { return absl::OkStatus(); }
};

TEST_F(LimitsTest, MaxColumnsPerTable) {
  std::string create_table_statement = "CREATE TABLE test_table (";
  for (int i = 0; i <= 1024; ++i) {
    absl::StrAppend(&create_table_statement, "int64_col_", i,
                    " INT64 NOT NULL, ");
  }
  absl::StrAppend(&create_table_statement, " ) PRIMARY KEY ()");

  EXPECT_THAT(UpdateSchema({create_table_statement}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest, MaxTableNameLength) {
  std::string create_table_statement = absl::StrCat(
      "CREATE TABLE test_table_name_greater_than_128_characters_",
      std::string(129, '0'), " ( ID INT64 NOT NULL, ) PRIMARY KEY (ID)");

  EXPECT_THAT(UpdateSchema({create_table_statement}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LimitsTest, MaxColumnNameLength) {
  std::string create_table_statement =
      absl::StrCat("CREATE TABLE test_table ( ID INT64 NOT NULL, ",
                   "test_column_name_greater_than_128_characters_",
                   std::string(129, '0'), " INT64,) PRIMARY KEY (ID)");

  EXPECT_THAT(UpdateSchema({create_table_statement}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LimitsTest, MaxIndexNameLength) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(MAX)
     ) PRIMARY KEY(int64_col)
  )"};
  statements.emplace_back(absl::StrCat(
      "CREATE INDEX long_test_index_name_greater_than_128_characters_",
      std::string(129, '0'), " ON test_table(string_col)"));

  EXPECT_THAT(UpdateSchema(statements),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// TODO: This test is timing out in the emulator.
TEST_F(LimitsTest, DISABLED_MaxTablesPerDatabase) {
  std::vector<std::string> statements;
  int i = 0;
  for (; i < 2560; ++i) {
    statements.emplace_back(
        absl::StrFormat("CREATE TABLE table_%d (ID INT64 NOT NULL, string_col "
                        "STRING(MAX),) PRIMARY KEY (ID)",
                        i));
  }
  // Add an index to verify that it is not counted towards table limit.
  statements.emplace_back("CREATE INDEX test_index ON table_0 (string_col)");
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  EXPECT_THAT(
      UpdateSchema({absl::StrFormat(
          "CREATE TABLE table_%d ( ID INT64 NOT NULL, ) PRIMARY KEY (ID)", i)}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

// TODO: This test is timing out in the emulator.
TEST_F(LimitsTest, DISABLED_MaxIndexesPerDatabase) {
  std::vector<std::string> statements;
  for (int i = 0; i < 80; ++i) {
    statements.emplace_back(
        absl::StrFormat("CREATE TABLE table_%d ( ID INT64 NOT NULL, string_col "
                        "STRING(MAX),) PRIMARY KEY (ID)",
                        i));
    // Max 64 index per table.
    for (int j = 0; j < 64; ++j) {
      statements.emplace_back(absl::StrFormat(
          "CREATE INDEX test_%d_index_%d ON table_%d(string_col)", i, j, i));
    }
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  // Adding one additional index to the database.
  EXPECT_THAT(
      UpdateSchema({"CREATE TABLE new_table ( ID INT64 NOT NULL, string_col "
                    "STRING(MAX),) PRIMARY KEY (ID)",
                    "CREATE INDEX new_index ON new_table(string_col)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest, MaxIndexPerTable) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(MAX),
     ) PRIMARY KEY(int64_col)
  )"};
  for (int i = 0; i < 64; ++i) {
    statements.emplace_back(
        absl::StrFormat("CREATE INDEX index_%d ON test_table(string_col)", i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  EXPECT_THAT(UpdateSchema({"CREATE INDEX index_64 ON test_table(string_col)"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest, MaxColumnInIndexPrimaryKey) {
  std::string create_table_statement = "CREATE TABLE test_table (";
  for (int i = 0; i <= 16; ++i) {
    absl::StrAppend(&create_table_statement, "int64_col_", i,
                    " INT64 NOT NULL, ");
  }
  absl::StrAppend(&create_table_statement, " ) PRIMARY KEY (int64_col_0)");
  std::string create_index_statement =
      "CREATE INDEX test_index ON test_table( ";
  for (int i = 0; i <= 16; ++i) {
    absl::StrAppend(&create_index_statement, "int64_col_", i);
    if (i < 16) {
      absl::StrAppend(&create_index_statement, ", ");
    }
  }
  absl::StrAppend(&create_index_statement, ")");

  EXPECT_THAT(UpdateSchema({create_table_statement, create_index_statement}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LimitsTest, MaxColumnsInTablePrimaryKey) {
  std::string create_table_statement = "CREATE TABLE test_table (";
  for (int i = 0; i <= 16; ++i) {
    absl::StrAppend(&create_table_statement, "int64_col_", i,
                    " INT64 NOT NULL, ");
  }
  absl::StrAppend(&create_table_statement, " ) PRIMARY KEY (");
  for (int i = 0; i <= 16; ++i) {
    absl::StrAppend(&create_table_statement, "int64_col_", i);
    if (i < 16) {
      absl::StrAppend(&create_table_statement, ", ");
    }
  }
  absl::StrAppend(&create_table_statement, " )");

  EXPECT_THAT(UpdateSchema({create_table_statement}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// TODO: Exceeding Strings and Bytes is an InvalidArguments error,
// but exceeding Array limits is FailedPrecondition. This needs to be handled in
// the emulator.
TEST_F(LimitsTest, DISABLED_MaxColumnSize) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       string_col_0 STRING(MAX),
       string_col_1 STRING(MAX),
     ) PRIMARY KEY(string_col_0)
  )"};
  ZETASQL_EXPECT_OK(UpdateSchema({statements}));

  // Fail primary key column exceeding max size.
  EXPECT_THAT(
      Insert("test_table", {"string_col_0"}, {std::string(10 << 20, 'a')}),
      StatusIs(absl::StatusCode::kInvalidArgument));

  // Fail non-primary key column exceeding max size.
  EXPECT_THAT(Insert("test_table", {"string_col_0", "string_col_1"},
                     {"b", std::string(10 << 20, 'b')}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LimitsTest, MaxPrimaryKeySizeOfTable) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       string_col_0 STRING(MAX),
       string_col_1 STRING(MAX),
     ) PRIMARY KEY(string_col_0, string_col_1)
  )"};
  ZETASQL_EXPECT_OK(UpdateSchema({statements}));

  // Fail single column exceeding primary key size.
  EXPECT_THAT(Insert("test_table", {"string_col_0", "string_col_1"},
                     {std::string(8192, 'a'), Null<std::string>()}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Fail multiple column exceeding the primary key size.
  EXPECT_THAT(Insert("test_table", {"string_col_0", "string_col_1"},
                     {std::string(4097, 'a'), std::string(4097, 'b')}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LimitsTest, MaxPrimaryKeySizeOfIndex) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       string_col_0 STRING(MAX),
       string_col_1 STRING(MAX),
       string_col_2 STRING(MAX),
     ) PRIMARY KEY(string_col_0)
  )",
                                         R"(
     CREATE INDEX test_index
     ON test_table(string_col_1, string_col_2)
  )"};
  ZETASQL_EXPECT_OK(UpdateSchema({statements}));

  // Fail single column exceeding primary key size.
  EXPECT_THAT(
      Insert("test_table", {"string_col_0", "string_col_1", "string_col_2"},
             {"1", std::string(8192, 'a'), Null<std::string>()}),
      StatusIs(absl::StatusCode::kInvalidArgument));

  // Fail multiple column exceeding the primary key size.
  EXPECT_THAT(
      Insert("test_table", {"string_col_0", "string_col_1", "string_col_2"},
             {"2", std::string(4097, 'a'), std::string(4097, 'b')}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LimitsTest, MaxTableInterleavingDepth) {
  std::vector<std::string> statements;
  int i = 0;
  for (; i < 7; ++i) {
    std::string create_table_statement = absl::StrFormat(
        "CREATE TABLE table_%d ( ID INT64 NOT NULL, ) PRIMARY KEY (ID) ", i);
    if (i > 0) {
      absl::StrAppend(&create_table_statement, ", INTERLEAVE IN PARENT ",
                      absl::StrFormat("table_%d", (i - 1)));
    }
    statements.emplace_back(create_table_statement);
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  EXPECT_THAT(UpdateSchema({absl::StrFormat(
                  "CREATE TABLE table_%d ( ID INT64 NOT NULL, ) PRIMARY KEY "
                  "(ID), INTERLEAVE IN PARENT table_%d",
                  i, (i - 1))}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
