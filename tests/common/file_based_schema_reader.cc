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

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/flags/flag.h"
#include "absl/strings/match.h"
#include "absl/strings/strip.h"
#include "tests/common/file_based_test_util.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

// States that the test case file parser can be in.
enum class ParserState {
  kReadingDialect,
  kReadingSchema,
};

using ::google::spanner::admin::database::v1::DatabaseDialect;

inline constexpr char kInvalidSchemaSetDefinitionError[] =
    ": schema set definition is invalid at line \"";

}  // namespace

absl::StatusOr<FileBasedSchemaSet> ReadSchemaSetFromFile(
    const std::string& file, const FileBasedSchemaSetOptions& options) {
  FileBasedSchemaSet schema_set(file);
  std::string line;
  ParserState state = ParserState::kReadingDialect;
  DatabaseDialect dialect;
  std::string schema;

  // Process the input file a line at a time.
  std::ifstream fin(file);
  while (std::getline(fin, line)) {
    switch (state) {
      case ParserState::kReadingDialect:
        if (absl::StartsWith(line, options.dialect_prefix)) {
          if (!::google::spanner::admin::database::v1::DatabaseDialect_Parse(
                  std::string(absl::StripPrefix(line, options.dialect_prefix)),
                  &dialect)) {
            return absl::InvalidArgumentError(absl::StrCat(
                file, kInvalidSchemaSetDefinitionError, line,
                "\". Dialect parameter must be one of ",
                "google.spanner.admin.database.v1.DatabaseDialect."));
          }
          if (dialect == DatabaseDialect::DATABASE_DIALECT_UNSPECIFIED) {
            return absl::InvalidArgumentError(absl::StrCat(
                file, kInvalidSchemaSetDefinitionError, line,
                "\". Dialect can't be DATABASE_DIALECT_UNSPECIFIED."));
          }
          if (schema_set.schemas.find(dialect) != schema_set.schemas.end()) {
            return absl::InvalidArgumentError(absl::StrCat(
                file, kInvalidSchemaSetDefinitionError, line,
                "\". File already defined a schema for this dialect."));
          }
          state = ParserState::kReadingSchema;
        } else if (absl::StartsWith(line, options.comment_prefix)) {
          // Do nothing.
        } else {
          return absl::InvalidArgumentError(absl::StrCat(
              file, kInvalidSchemaSetDefinitionError, line,
              "\". Expected either a dialect parameter or a comment."));
        }
        break;
      case ParserState::kReadingSchema:
        if (absl::StartsWith(line, options.dialect_prefix)) {
          return absl::InvalidArgumentError(absl::StrCat(
              file, kInvalidSchemaSetDefinitionError, line,
              "\". Expected a schema delimiter before the next dialect ",
              "parameter."));
        } else if (line == options.schema_delimiter) {
          schema_set.schemas[dialect] = schema;
          schema = "";
          state = ParserState::kReadingDialect;
        } else if (absl::StartsWith(line, options.comment_prefix)) {
          // Do nothing.
        } else {
          // Add this line to the schema set.
          schema += line + "\n";
        }
        break;
    }
  }

  // Add the final schema if any.
  if (!schema.empty()) {
    schema_set.schemas[dialect] = schema;
  }

  if (schema_set.schemas.empty()) {
    return absl::InvalidArgumentError("Schema file doesn't exist or is empty.");
  }

  return schema_set;
}

std::string GetRunfilesDir(const std::string& dir) {
  return GetTestFileDir(
      absl::StrCat("com_google_cloud_spanner_emulator", "/", dir));
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
