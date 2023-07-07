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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_SCHEMA_READER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_SCHEMA_READER_H_

// The file-based schema reader can read a file containing schema definitions
// for one or more dialects. The file must be formatted as follows:
//
// @Dialect=<GOOGLE_STANDARD_SQL|POSTGRESQL>
// # <some comment>
// <schema line 1>
// <schema line 2>
// ...
// ===
// # <another comment>
// @Dialect=<GOOGLE_STANDARD_SQL|POSTGRESQL>
// <schema line 1>
// <schema line 2>
// ...
//
// Example schema file:
//
// @Dialect=GOOGLE_STANDARD_SQL
// CREATE TABLE users(
//   user_id    INT64 NOT NULL,
//   name       STRING(MAX),
//   age        INT64,
// ) PRIMARY KEY (user_id);
// ===
// @Dialect=POSTGRESQL
// CREATE TABLE users(
//   user_id    bigint NOT NULL PRIMARY KEY,
//   Name       varchar,
//   Age        bigint
// );

#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "absl/status/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Options for the file based schemas.
struct FileBasedSchemaSetOptions {
  // Specifies the dialect for the schema that follows.
  // The dialect value that follows should be one of
  // google.spanner.admin.database.v1.DatabaseDialect.
  // E.g. Dialect=GOOGLE_STANDARD_SQL
  // NOTE that DATABASE_DIALECT_UNSPECIFIED is invalid.
  std::string dialect_prefix = "@Dialect=";

  // Delimiter for the schemas across dialects.
  std::string schema_delimiter = "===";

  // Prefix for comments.
  std::string comment_prefix = "# ";
};

// Container for a schema set.
struct FileBasedSchemaSet {
  FileBasedSchemaSet(absl::string_view file_name) {}
  std::map<::google::spanner::admin::database::v1::DatabaseDialect, std::string>
      schemas;
};

// Reads and returns all testcases in `file`.
absl::StatusOr<FileBasedSchemaSet> ReadSchemaSetFromFile(
    const std::string& file, const FileBasedSchemaSetOptions& options);

// Returns the runfiles directory for the given source-root relative directory.
std::string GetRunfilesDir(const std::string& dir);

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_FILE_BASED_SCHEMA_READER_H_
