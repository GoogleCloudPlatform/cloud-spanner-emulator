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

#include "tests/common/file_based_schema_reader.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"

namespace {

using ::google::spanner::admin::database::v1::DatabaseDialect;
using google::spanner::emulator::test::FileBasedSchemaSet;
using google::spanner::emulator::test::FileBasedSchemaSetOptions;
using google::spanner::emulator::test::GetRunfilesDir;

class FileBasedSchemaReaderTest : public ::testing::Test {
 protected:
  absl::StatusOr<FileBasedSchemaSet> GetSchemaSet(const std::string file) {
    const std::string root_dir = GetRunfilesDir("tests/common/testdata");
    const std::string file_path = absl::StrCat(root_dir, "/", file);

    return ReadSchemaSetFromFile(file_path, FileBasedSchemaSetOptions{});
  }
};

TEST_F(FileBasedSchemaReaderTest, BothDialects) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema_set, GetSchemaSet("multiple_dialects.test"));
  EXPECT_EQ(schema_set.schemas.size(), 2);
  EXPECT_FALSE(
      schema_set.schemas[DatabaseDialect::GOOGLE_STANDARD_SQL].empty());
  EXPECT_FALSE(schema_set.schemas[DatabaseDialect::POSTGRESQL].empty());
}

TEST_F(FileBasedSchemaReaderTest, GSQLDialect) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema_set, GetSchemaSet("gsql_only.test"));
  EXPECT_EQ(schema_set.schemas.size(), 1);
  EXPECT_FALSE(
      schema_set.schemas[DatabaseDialect::GOOGLE_STANDARD_SQL].empty());
  EXPECT_EQ(schema_set.schemas.find(DatabaseDialect::POSTGRESQL),
            schema_set.schemas.end());
}

TEST_F(FileBasedSchemaReaderTest, PGDialect) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema_set, GetSchemaSet("pg_only.test"));
  EXPECT_EQ(schema_set.schemas.size(), 1);
  EXPECT_FALSE(schema_set.schemas[DatabaseDialect::POSTGRESQL].empty());
  EXPECT_EQ(schema_set.schemas.find(DatabaseDialect::GOOGLE_STANDARD_SQL),
            schema_set.schemas.end());
}

TEST_F(FileBasedSchemaReaderTest, InvalidDialectParameter) {
  EXPECT_THAT(GetSchemaSet("invalid_dialect.test"),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Dialect parameter must be one of")));
}

TEST_F(FileBasedSchemaReaderTest, InvalidUnspecifiedDialect) {
  EXPECT_THAT(GetSchemaSet("invalid_unspecified_dialect.test"),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("DATABASE_DIALECT_UNSPECIFIED")));
}

TEST_F(FileBasedSchemaReaderTest, InvalidDuplicateDialect) {
  EXPECT_THAT(
      GetSchemaSet("invalid_duplicate_dialect.test"),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("already defined a schema for this dialect")));
}

TEST_F(FileBasedSchemaReaderTest, InvalidWithoutDialect) {
  EXPECT_THAT(GetSchemaSet("invalid_no_dialect.test"),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("either a dialect parameter")));
}

TEST_F(FileBasedSchemaReaderTest, InvalidWithoutSchemaDelimiter) {
  EXPECT_THAT(GetSchemaSet("invalid_without_schema_delimiter.test"),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Expected a schema delimiter")));
}

TEST_F(FileBasedSchemaReaderTest, InvalidFileName) {
  EXPECT_THAT(GetSchemaSet("some_file.test"),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Schema file doesn't exist")));
}

}  // namespace
