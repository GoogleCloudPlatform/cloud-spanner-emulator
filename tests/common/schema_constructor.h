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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCHEMA_CONSTRUCTOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCHEMA_CONSTRUCTOR_H_

#include <memory>

#include "zetasql/public/type.h"
#include "absl/memory/memory.h"
#include "zetasql/base/statusor.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Utility methods for initializing standard schemas for unit tests.

// Creates a schema from supplied DDL statements.
// Note:Does not perform any backfill/verification tasks.
//
// TODO : Deprecate this method and fix all tests.
inline zetasql_base::StatusOr<std::unique_ptr<const backend::Schema>>
CreateSchemaFromDDL(absl::Span<const std::string> statements,
                    zetasql::TypeFactory* type_factory) {
  backend::TableIDGenerator table_id_gen;
  backend::ColumnIDGenerator column_id_gen;
  backend::SchemaChangeContext context{
      .type_factory = type_factory,
      .table_id_generator = &table_id_gen,
      .column_id_generator = &column_id_gen,
  };
  backend::SchemaUpdater updater;
  return updater.ValidateSchemaFromDDL(statements, context);
}

// Creates a schema with a single table and an index on the table.
inline std::unique_ptr<const backend::Schema> CreateSchemaWithOneTable(
    zetasql::TypeFactory* type_factory) {
  auto maybe_schema = CreateSchemaFromDDL(
      {
          R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
          R"(
              CREATE UNIQUE INDEX test_index ON test_table(string_col DESC)
            )",
      },
      type_factory);
  return std::move(maybe_schema.ValueOrDie());
}

// Creates a schema with two child tables interleaved in a parent table.
inline std::unique_ptr<const backend::Schema> CreateSchemaWithInterleaving(
    zetasql::TypeFactory* const type_factory) {
  auto maybe_schema = CreateSchemaFromDDL(
      {
          R"(
              CREATE TABLE Parent (
                k1 INT64 NOT NULL,
                c1 STRING(MAX)
              ) PRIMARY KEY (k1)
            )",
          R"(
              CREATE TABLE CascadeDeleteChild (
                k1 INT64 NOT NULL,
                k2 INT64 NOT NULL,
                c1 STRING(MAX)
              ) PRIMARY KEY (k1, k2),
                INTERLEAVE IN PARENT Parent ON DELETE CASCADE
            )",
          R"(
              CREATE TABLE NoActionDeleteChild (
                k1 INT64 NOT NULL,
                k2 INT64 NOT NULL,
                c1 STRING(MAX)
              ) PRIMARY KEY (k1, k2),
                INTERLEAVE IN PARENT Parent ON DELETE NO ACTION
            )",
      },
      type_factory);
  return std::move(maybe_schema.ValueOrDie());
}

// Creates a schema with two top level tables and one child table.
inline std::unique_ptr<const backend::Schema> CreateSchemaWithMultiTables(
    zetasql::TypeFactory* type_factory) {
  auto maybe_schema = CreateSchemaFromDDL(
      {
          R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
          R"(
              CREATE TABLE child_table (
                int64_col INT64 NOT NULL,
                child_key INT64 NOT NULL,
              ) PRIMARY KEY (int64_col, child_key),
              INTERLEAVE IN PARENT test_table ON DELETE CASCADE
            )",
          R"(
              CREATE TABLE test_table2 (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
      },
      type_factory);
  return std::move(maybe_schema.ValueOrDie());
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCHEMA_CONSTRUCTOR_H_
