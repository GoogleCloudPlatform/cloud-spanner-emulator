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
#include <string>

#include "zetasql/public/type.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace database_api = ::google::spanner::admin::database::v1;

// Utility methods for initializing standard schemas for unit tests.

// Creates a schema from supplied DDL statements.
// Note:Does not perform any backfill/verification tasks.
//
// TODO : Deprecate this method and fix all tests.
absl::StatusOr<std::unique_ptr<const backend::Schema>> CreateSchemaFromDDL(
    absl::Span<const std::string> statements,
    zetasql::TypeFactory* type_factory
    ,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

// Creates a schema with a single table and an index on the table.
std::unique_ptr<const backend::Schema> CreateSchemaWithOneTable(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

std::unique_ptr<const backend::Schema>
CreateSchemaWithOneTableAndOneChangeStream(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

absl::StatusOr<std::unique_ptr<const backend::Schema>>
CreateSchemaWithOneSequence(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

std::unique_ptr<const backend::Schema> CreateSchemaWithOneModel(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

std::unique_ptr<const backend::Schema> CreateSimpleDefaultValuesSchema(
    zetasql::TypeFactory* type_factory);

std::unique_ptr<const backend::Schema> CreateSchemaWithOneTableWithSynonym(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

// Creates a schema with a single table and generated primary key column.
inline absl::StatusOr<std::unique_ptr<const backend::Schema>>
CreateGpkSchemaWithOneTable(zetasql::TypeFactory* type_factory) {
  return CreateSchemaFromDDL(
      {
          R"(
              CREATE TABLE test_table (
                k1_pk INT64 NOT NULL,
                k2 INT64 NOT NULL,
                k3gen_storedpk INT64 NOT NULL AS (k2) STORED,
                k4 INT64,
                k5 INT64 AS (k4+1) STORED,
              ) PRIMARY KEY (k1_pk,k3gen_storedpk)
            )",
      },
      type_factory);
}

// Creates a schema with two child tables interleaved in a parent table.
std::unique_ptr<const backend::Schema> CreateSchemaWithInterleaving(
    zetasql::TypeFactory* const type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

// Creates a schema with two top level tables and one child table.
std::unique_ptr<const backend::Schema> CreateSchemaWithMultiTables(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

// Creates a schema with foreign key constraints.
std::unique_ptr<const backend::Schema> CreateSchemaWithForeignKey(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

// Creates a schema with foreign key constraints that have ON DELETE clauses.
std::unique_ptr<const backend::Schema> CreateSchemaWithForeignKeyOnDelete(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect =
        database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);

std::unique_ptr<const backend::Schema> CreateSchemaWithView(
    zetasql::TypeFactory* type_factory);

std::unique_ptr<const backend::Schema> CreateSchemaWithNamedSchema(
    zetasql::TypeFactory* type_factory);

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_SCHEMA_CONSTRUCTOR_H_
