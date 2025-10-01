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

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "google/cloud/optional.h"
#include "google/cloud/spanner/bytes.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/transaction.h"
#include "google/cloud/spanner/value.h"
#include "tests/common/file_based_test_runner.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using cloud::spanner::Bytes;
using cloud::spanner::InsertMutationBuilder;
using ::google::spanner::admin::database::v1::DatabaseDialect;
using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using zetasql_base::testing::StatusIs;

template <class T>
using optional = google::cloud::optional<T>;

// Directory containing schema change test files.
const char kSchemaChangeTestDataDir[] = "tests/conformance/data/schema_changes";

// Directory containing PostgreSQL DDL test files.
const char kPgDdlTestDataDir[] = "tests/conformance/data/schema_changes/pg";

// List of schema change test files within the directory above.
const absl::string_view kSchemaChangeTestFiles[] = {
    "change_streams.test",
    "check_constraint.test",
    "column_default_values.test",
    "combined.test",
    "foreign_key.test",
    "generated_column.test",
    "key_column_alteration.test",
    "models.test",
    "search.test",
    "views.test",
};

const absl::string_view kPgDdlTestFiles[] = {
    "ddl.create_index.test",
    "ddl.create_table.test",
    "ddl.create_table_checkconstraint.test",
    "ddl.create_table_generatedcolumn_default.test",
    "ddl.create_table_generatedprimarykey.test",
    "ddl.create_view.test",
    "ddl.create_change_stream.test",
    "ddl.foreign_keys.test",
    "ddl.alter_table.test",
    "ddl.alter_table_generatedcolumn_default.test",
    "ddl.alter_change_stream.test",
    "ddl.alter_index.test",
    "ddl.drop_index.test",
    "ddl.drop_table.test",
    "ddl.drop_view.test",
    "ddl.drop_change_stream.test",
};

constexpr std::array<char, 10> kBytesLiteral = {'\xd0', '\xb0', '\xd0', '\xb1',
                                                '\xd0', '\xb2', '\xd0', '\xb3',
                                                '\xd0', '\xb4'};

const char kUnicodeStringLiteral[] = "абвгд";

class SchemaChangeTest
    : public DatabaseTest,
      public ::testing::WithParamInterface<FileBasedTestCase> {
 public:
  SchemaChangeTest()
      : flag_setter_({
            .enable_generated_pk = true,
            .enable_postgresql_interface = true,
            .enable_fk_delete_cascade_action = true,
            .enable_fk_enforcement_option = true,
            .enable_interleave_in = true,
        }) {
  }
  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

  // Runs a file-based schema change test case.
  absl::StatusOr<FileBasedTestCaseOutput> RunSchemaChangeTestCase(
      const FileBasedTestCaseInput& input) {
    // Check that we were not mistakenly passed an empty test case.
    ZETASQL_RET_CHECK(!input.text.empty()) << "Found empty schema change test case.";

    dialect_ = input.dialect;

    // Reset the database used for this test.
    ZETASQL_RETURN_IF_ERROR(ResetDatabase());

    // Split the input into individual DDL statements.
    std::string text = input.text;
    absl::StripAsciiWhitespace(&text);
    std::vector<std::string> input_statements =
        absl::StrSplit(text, ';', absl::SkipEmpty());

    // Run the update.
    absl::Status status = UpdateSchema(input_statements).status();

    // For the error case, we expect the error string to match.
    if (!status.ok()) {
      // Strip out the prefix added by the C++ client library.
      absl::string_view message(status.message());
      absl::ConsumePrefix(&message,
                          "Error in non-idempotent operation UpdateDatabase: ");

      // Return the expected error message.
      return FileBasedTestCaseOutput{
          .text = "ERROR: " + std::string(message) + "\n",
          .expect_error = true,
          .status_code = status.code()};
    }

    // For the success case, we expect the output of GetDatabaseDdl to match.
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::string> output_statements,
                     GetDatabaseDdl());
    return FileBasedTestCaseOutput{
        .text = absl::StrJoin(output_statements, ";\n") + ";\n",
        .status_code = absl::StatusCode::kOk};
  }

  // Returns list of all schema change test files.
  static std::vector<std::string> GetAllTestFiles(DatabaseDialect dialect) {
    std::vector<std::string> result;
    std::string root_dir = dialect == DatabaseDialect::POSTGRESQL
                               ? GetRunfilesDir(kPgDdlTestDataDir)
                               : GetRunfilesDir(kSchemaChangeTestDataDir);
    if (dialect == DatabaseDialect::POSTGRESQL) {
      for (absl::string_view filename : kPgDdlTestFiles) {
        result.push_back(absl::StrCat(root_dir, "/", filename));
      }
    } else {
      for (absl::string_view filename : kSchemaChangeTestFiles) {
        result.push_back(absl::StrCat(root_dir, "/", filename));
      }
    }
    return result;
  }

  // Returns list of all test cases from all the test case files.
  static std::vector<FileBasedTestCase> GetAllTestCases() {
    std::vector<FileBasedTestCase> test_cases;
    for (DatabaseDialect dialect :
         {DatabaseDialect::POSTGRESQL, DatabaseDialect::GOOGLE_STANDARD_SQL}) {
      for (const auto& file : GetAllTestFiles(dialect)) {
        const auto file_cases =
            ReadTestCasesFromFile(file, FileBasedTestOptions{}, dialect);
        test_cases.insert(test_cases.end(), file_cases.begin(),
                          file_cases.end());
      }
    }
    return test_cases;
  }
};

TEST_P(SchemaChangeTest, FileBasedTests) {
  const auto& input = GetParam().input;
  const std::string input_line_message = absl::StrCat(
      "for input at line number ", input.line_no, ":\n", input.text);
  const auto& expected = GetParam().expected;

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto actual, RunSchemaChangeTestCase(input));
  ASSERT_TRUE(actual.status_code.has_value());

  std::string expected_text = expected.text;
  std::string actual_text = actual.text;

  if (expected.expect_error) {
    // Match the status code if present.
    if (expected.status_code.has_value()) {
      EXPECT_THAT(actual.status_code, testing::Eq(*expected.status_code))
          << input_line_message;
    }
  }

  auto normalize = [](std::string* text) {
    RE2::GlobalReplace(text, "\n", " ");
    RE2::GlobalReplace(text, R"(\\n)", " ");
    RE2::GlobalReplace(text, R"(\\')", "'");
    RE2::GlobalReplace(text, R"(\\")", "\"");
    absl::RemoveExtraAsciiWhitespace(text);
  };
  normalize(&actual_text);
  normalize(&expected_text);
  if (input.regex) {
    EXPECT_THAT(actual_text, testing::MatchesRegex(expected_text))
        << input_line_message;
  } else {
    EXPECT_THAT(actual_text, testing::HasSubstr(expected_text))
        << input_line_message;
  }
}

INSTANTIATE_TEST_SUITE_P(
    FileBasedTest, SchemaChangeTest,
    testing::ValuesIn(SchemaChangeTest::GetAllTestCases()),
    [](const testing::TestParamInfo<FileBasedTestCase>& info) {
      const auto& input = info.param.input;
      int start = input.file_name.find_last_of('/') + 1;
      int limit = input.file_name.find_last_of('.');
      return absl::StrReplaceAll(
          absl::StrCat(input.file_name.substr(start, limit - start), "_",
                       input.line_no),
          {{".", "_"}});
    });

TEST_F(SchemaChangeTest, NoStatements) {
  EXPECT_THAT(UpdateSchema(/*schema=*/{}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SchemaChangeTest, ValidStatements) {
  UpdateDatabaseDdlMetadata metadata;
  std::vector<std::string> statements = {R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(MAX)
     ) PRIMARY KEY(int64_col)
  )",
                                         R"(
     CREATE INDEX test_index ON test_table(string_col)
  )"};

  ZETASQL_ASSERT_OK_AND_ASSIGN(metadata, UpdateSchema(statements));
  EXPECT_EQ(metadata.commit_timestamps_size(), 2);
  EXPECT_EQ(metadata.statements_size(), 2);
}
TEST_F(SchemaChangeTest, DISABLED_PartialSuccess) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
              CREATE TABLE test_table(
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY(int64_col) )"}));

  auto mutation =
      InsertMutationBuilder("test_table", {"int64_col", "string_col"})
          .AddRow({Value(1), Value("a")})
          .AddRow({Value(2), Value("a")})
          .Build();
  auto txn = Transaction(Transaction::ReadWriteOptions());
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {mutation}));

  std::vector<std::string> statements = {R"(
     CREATE TABLE another_table (
       int64_col INT64 NOT NULL,
     ) PRIMARY KEY (int64_col)
  )",
                                         R"(
     CREATE UNIQUE INDEX test_index ON test_table(string_col)
  )"};

  EXPECT_THAT(UpdateSchema(statements),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto ddl_statements, GetDatabaseDdl());
  EXPECT_EQ(ddl_statements.size(), 2);
}

TEST_F(SchemaChangeTest, AddColumns) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
     ) PRIMARY KEY(int64_col)
  )"}));
  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col"}, {Value(1)}));

  ZETASQL_EXPECT_OK(UpdateSchema(
      {"ALTER TABLE test_table ADD COLUMN string_col STRING(MAX)"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE test_table "
                    "ADD COLUMN gen_int64_col INT64 AS (int64_col) STORED"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE test_table "
                    "ADD COLUMN default_int64_col INT64 DEFAULT (1)"}));
}

TEST_F(SchemaChangeTest, AddColumnsWithoutKeyword) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
     ) PRIMARY KEY(int64_col)
  )"}));
  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col"}, {Value(1)}));

  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE test_table ADD string_col STRING(MAX)"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE test_table "
                    "ADD gen_int64_col INT64 AS (int64_col) STORED"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE test_table "
                    "ADD default_int64_col INT64 DEFAULT (1)"}));
}

TEST_F(SchemaChangeTest, AddDropColumns) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
     ) PRIMARY KEY(int64_col)
  )"}));
  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col"}, {Value(1)}));

  // Test with and without COLUMN keyword.
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE test_table ADD COLUMN col_A STRING(MAX)"}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE test_table ADD col_B STRING(MAX)"}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE test_table DROP COLUMN col_A"}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE test_table DROP col_B"}));
}

TEST_F(SchemaChangeTest, AlterColumnTypeChange) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(30)
     ) PRIMARY KEY(int64_col)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_col"},
                   {Value(1), Value("abcdefghijklmnopqrstuvwxyz")}));

  // Check for invalid size reduction when converting from STRING to BYTES.
  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_col BYTES(10)
  )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(SchemaChangeTest, AlterColumnTypeChangeWithoutKeyword) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(30)
     ) PRIMARY KEY(int64_col)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_col"},
                   {Value(1), Value("abcdefghijklmnopqrstuvwxyz")}));

  // Check for invalid size reduction when converting from STRING to BYTES.
  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER string_col BYTES(10)
  )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(SchemaChangeTest, AlterColumnSizeReduction) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(30)
     ) PRIMARY KEY(int64_col)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_col"},
                   {Value(1), Value("abcdefghijklmnopqrstuvwxyz")}));

  // Cannot reduce size below 26.
  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_col STRING(10)
  )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(SchemaChangeTest, AlterColumnInvalidTypeChange) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(30)
     ) PRIMARY KEY(int64_col)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_col"},
                   {Value(1), Value("abcdefghijklmnopqrstuvwxyz")}));

  // Cannot change from STRING to BOOL.
  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_col BOOL
  )"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SchemaChangeTest, AlterColumnUTFInvalid) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       bytes_col BYTES(30)
     ) PRIMARY KEY(int64_col)
  )"}));

  // This is not a valid UTF8 encoding.
  const unsigned char byte_val[] = {0xFF, 0xFF, 0xFF, 0xFF};
  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "bytes_col"},
                   {Value(1), Value(Bytes(byte_val))}));

  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN bytes_col STRING(30)
  )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(SchemaChangeTest, AlterColumnStringToBytes) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(5)
     ) PRIMARY KEY(int64_col)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_col"},
                   {Value(1), Value("абвгд")}));

  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_col BYTES(6)
  )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  ZETASQL_EXPECT_OK(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_col BYTES(10)
  )"}));

  // Check that the type was changed correctly.
  EXPECT_THAT(Read("test_table", {"string_col"}, KeySet::All()),
              IsOkAndHoldsRow({Bytes(kBytesLiteral)}));
}

TEST_F(SchemaChangeTest, AlterColumnWithNullValues) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_col STRING(5)
     ) PRIMARY KEY(int64_col)
  )"}));

  // Explicit NULL.
  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_col"},
                   {Value(1), cloud::spanner::MakeNullValue<std::string>()}));

  // Implicit NULL.
  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col"}, {Value(2)}));

  // Update succeeds since all the column values are NULL.
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_col BYTES(2)
  )"}));

  // Check that the type was changed correctly.
  EXPECT_THAT(Read("test_table", {"string_col"}, KeySet::All()),
              IsOkAndHoldsRows({{Null<Bytes>()}, {Null<Bytes>()}}));
}

TEST_F(SchemaChangeTest, AlterColumnBytesToString) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       bytes_col BYTES(30)
     ) PRIMARY KEY(int64_col)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "bytes_col"},
                   {Value(1), Bytes(kBytesLiteral)}));

  // This will fail as the byte-sequence translates to a 5-(unicode)char string.
  EXPECT_THAT(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN bytes_col STRING(4)
  )"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  ZETASQL_EXPECT_OK(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN bytes_col STRING(11)
  )"}));

  // Check that the type was changed correctly.
  EXPECT_THAT(Read("test_table", {"bytes_col"}, KeySet::All()),
              IsOkAndHoldsRow({Value("абвгд")}));
}

TEST_F(SchemaChangeTest, AlterColumnArrayType) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
       int64_col INT64 NOT NULL,
       string_arr_col ARRAY<STRING(30)>
     ) PRIMARY KEY(int64_col)
  )"}));

  std::vector<optional<std::string>> string_arr{kUnicodeStringLiteral,
                                                optional<std::string>()};
  std::vector<optional<Bytes>> bytes_arr{Bytes(kBytesLiteral),
                                         optional<Bytes>()};

  ZETASQL_ASSERT_OK(Insert("test_table", {"int64_col", "string_arr_col"},
                   {Value(1), Value(string_arr)}));

  ZETASQL_EXPECT_OK(UpdateSchema({R"(
     ALTER TABLE test_table ALTER COLUMN string_arr_col ARRAY<BYTES(10)>
  )"}));

  // Check that the type was changed correctly.
  EXPECT_THAT(Read("test_table", {"string_arr_col"}, KeySet::All()),
              IsOkAndHoldsRow({Value(bytes_arr)}));
}

TEST_F(SchemaChangeTest, Analyze) { ZETASQL_EXPECT_OK(UpdateSchema({"ANALYZE"})); }

TEST_F(SchemaChangeTest, UpdateWithNonKeyDefaultColumns) {
  ZETASQL_EXPECT_OK(SetSchema({R"(
     CREATE TABLE test_table (
      id STRING(36) NOT NULL DEFAULT (GENERATE_UUID()),
      str STRING(MAX) NOT NULL,
      bool BOOL NOT NULL DEFAULT (false)
    ) PRIMARY KEY (id)
  )"}));

  ZETASQL_ASSERT_OK(Insert("test_table", {"id", "str"}, {Value(1), Value("test")}));
  ZETASQL_ASSERT_OK(Update("test_table", {"id", "str"}, {Value(1), Value("updated")}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
