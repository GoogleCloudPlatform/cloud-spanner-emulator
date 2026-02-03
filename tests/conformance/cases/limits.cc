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

#include "common/limits.h"

#include <string>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "google/cloud/spanner/bytes.h"
#include "tests/conformance/common/database_test_base.h"
#include "tests/conformance/common/environment.h"

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

TEST_F(LimitsTest, MaxChangeStreamNameLength) {
  std::string create_change_stream_statement = absl::StrCat(
      "CREATE CHANGE STREAM "
      "test_change_stream_name_greater_than_128_characters_",
      std::string(129, '0'), " FOR ALL");

  EXPECT_THAT(UpdateSchema({create_change_stream_statement}),
              StatusIs(absl::StatusCode::kInvalidArgument));
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

TEST_F(LimitsTest, MaxChangeStreamsPerDatabase) {
  std::vector<std::string> statements;
  int i = 0;
  for (; i < 10; ++i) {
    statements.emplace_back(
        absl::StrFormat("CREATE TABLE table_%d (ID INT64 NOT NULL, string_col "
                        "STRING(MAX),) PRIMARY KEY (ID)",
                        i));
    statements.emplace_back(absl::StrFormat(
        "CREATE CHANGE STREAM change_stream_%d FOR table_%d", i, i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));
  ZETASQL_ASSERT_OK(
      UpdateSchema({absl::StrFormat("CREATE TABLE table_%d (ID INT64 NOT NULL, "
                                    "string_col STRING(MAX),) PRIMARY KEY (ID)",
                                    i)}));
  EXPECT_THAT(UpdateSchema({absl::StrFormat(
                  "CREATE CHANGE STREAM change_stream_%d FOR table_%d", i, i)}),
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
  for (int i = 0; i < 128; ++i) {
    statements.emplace_back(
        absl::StrFormat("CREATE INDEX index_%d ON test_table(string_col)", i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  EXPECT_THAT(
      UpdateSchema({"CREATE INDEX index_too_many ON test_table(string_col)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest, MaxChangeStreamsPerTable) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(MAX),
     ) PRIMARY KEY(int64_col)
  )"};
  for (int i = 0; i < 3; ++i) {
    statements.emplace_back(absl::StrFormat(
        "CREATE CHANGE STREAM change_stream_%d FOR test_table", i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  EXPECT_THAT(
      UpdateSchema({"CREATE CHANGE STREAM change_stream_too_many FOR ALL"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest,
       CreateChangeStreamsTrackingPkColsOnlyNotCountTowardCSNumLimitPerObject) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       bool_col BOOL NOT NULL,
       string_col1 STRING(MAX),
       string_col2 STRING(MAX),
     ) PRIMARY KEY(int64_col, bool_col)
  )"};
  for (int i = 0; i < 3; ++i) {
    statements.emplace_back(absl::StrFormat(
        "CREATE CHANGE STREAM change_stream_%d FOR test_table()", i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE CHANGE STREAM change_stream_3 FOR test_table()"}));
  ZETASQL_EXPECT_OK(UpdateSchema({"CREATE CHANGE STREAM change_stream_all FOR ALL"}));
  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE CHANGE STREAM change_stream_test_table FOR test_table"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE CHANGE STREAM change_stream_test_table_string_col1 "
                    "FOR test_table(string_col1)"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE CHANGE STREAM change_stream_test_table_string_col2 "
                    "FOR test_table(string_col2)"}));
  EXPECT_THAT(
      UpdateSchema(
          {"CREATE CHANGE STREAM change_stream_test_table_string_col1_again "
           "FOR test_table(string_col1)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest,
       AlterChangeStreamsTrackingPkColsOnlyNotCountTowardCSNumLimitPerObject) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       bool_col BOOL NOT NULL,
       string_col STRING(MAX),
     ) PRIMARY KEY(int64_col, bool_col)
  )"};
  for (int i = 0; i < 3; ++i) {
    statements.emplace_back(absl::StrFormat(
        "CREATE CHANGE STREAM change_stream_%d FOR test_table", i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));
  EXPECT_THAT(
      UpdateSchema({"CREATE CHANGE STREAM change_stream_too_many FOR ALL"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
  ZETASQL_EXPECT_OK(UpdateSchema(
      {"ALTER CHANGE STREAM change_stream_0 SET FOR test_table()"}));
  ZETASQL_EXPECT_OK(UpdateSchema({"CREATE CHANGE STREAM change_stream_all FOR ALL"}));
}

TEST_F(LimitsTest,
       ChangeStreamsTrackingPkColsOnlyStillCountTowardMaxCSNumPerDataBase) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(MAX),
     ) PRIMARY KEY(int64_col)
  )"};
  for (int i = 0; i < 10; ++i) {
    statements.emplace_back(absl::StrFormat(
        "CREATE CHANGE STREAM change_stream_%d FOR test_table()", i));
  }
  ZETASQL_EXPECT_OK(UpdateSchema(statements));

  EXPECT_THAT(
      UpdateSchema({"CREATE CHANGE STREAM change_stream_too_many FOR ALL"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(LimitsTest, AllowFourChangeStreamsTrackingDiffNonKeyColOfSameTable) {
  std::vector<std::string> statements = {
      R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col1 STRING(MAX),
       string_col2 STRING(MAX),
       string_col3 STRING(MAX),
       string_col4 STRING(MAX),
     ) PRIMARY KEY(int64_col)
  )",
      R"(CREATE CHANGE STREAM cs1 FOR test_table(string_col1))",
      R"(CREATE CHANGE STREAM cs2 FOR test_table(string_col2))",
      R"(CREATE CHANGE STREAM cs3 FOR test_table(string_col3))"};
  ZETASQL_EXPECT_OK(UpdateSchema(statements));
  ZETASQL_EXPECT_OK(UpdateSchema(
      {R"(CREATE CHANGE STREAM cs4 FOR test_table(string_col4))"}));
  ZETASQL_EXPECT_OK(UpdateSchema({R"(CREATE CHANGE STREAM cs_all FOR ALL)"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({R"(CREATE CHANGE STREAM cs_test_table FOR test_table)"}));
}

TEST_F(LimitsTest, MaxChangeStreamsPerColumn) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(MAX),
     ) PRIMARY KEY(int64_col)
  )"};
  for (int i = 0; i < 4; i++) {
    statements.emplace_back(absl::StrFormat(
        "CREATE CHANGE STREAM change_stream_%d FOR test_table(string_col)", i));
  }
  EXPECT_THAT(UpdateSchema(statements),
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

TEST_F(LimitsTest, MaxValueSize) {
  std::vector<std::string> statements = {R"(
     CREATE TABLE max_value_size_test_table (
       int64_col INT64 NOT NULL,
       bytes_col BYTES(MAX),
     ) PRIMARY KEY(int64_col)
  )"};
  ZETASQL_EXPECT_OK(UpdateSchema({statements}));

  auto bytes_col_value = google::spanner::emulator::test::ToUtilStatusOr(
      cloud::spanner_internal::BytesFromBase64(
          std::string(limits::kMaxBytesColumnLength, 'a')));
  ZETASQL_ASSERT_OK(bytes_col_value);

  ZETASQL_EXPECT_OK(Insert("max_value_size_test_table", {"int64_col", "bytes_col"},
                   {1, *bytes_col_value}));

  // Construct a struct object with a field value of 10MB.
  // This is the maximum size that can be inserted into a bytes column.
  auto result = Query(
      absl::StrFormat("SELECT ARRAY(SELECT AS STRUCT bytes_col as bytes_col "
                      "FROM max_value_size_test_table);"));
  ZETASQL_ASSERT_OK(result);
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ(result->at(0).values().size(), 1);
  Value array_value = result->at(0).values()[0];
  auto struct_array = google::spanner::emulator::test::ToUtilStatusOr(
      array_value.get<std::vector<std::tuple<Bytes>>>());
  ZETASQL_ASSERT_OK(struct_array);
  ASSERT_EQ(struct_array->size(), 1);
  auto struct_value = struct_array->at(0);
  ASSERT_EQ(std::get<0>(struct_value), *bytes_col_value);
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
