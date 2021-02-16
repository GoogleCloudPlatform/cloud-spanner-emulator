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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_PARSER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_PARSER_H_

#include "zetasql/base/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/schema/ddl/operations.pb.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

// The option to enable the use of cloud spanner commit timestamps for a column.
extern const char kCommitTimestampOptionName[];

// Parses a create database statement.
zetasql_base::StatusOr<CreateDatabase> ParseCreateDatabase(
    absl::string_view create_statement);

// Parses a single DDL statement.
zetasql_base::StatusOr<DDLStatement> ParseDDLStatement(absl::string_view input);

// Parses a list of DDL statements.
zetasql_base::StatusOr<Schema> ParseDDLStatements(
    absl::Span<const absl::string_view> statements);

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_PARSER_H_
