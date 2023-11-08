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

#include "backend/schema/updater/schema_updater_tests/base.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/schema/parser/ddl_parser.h"
#include "backend/schema/updater/schema_updater.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

absl::StatusOr<std::unique_ptr<const Schema>> SchemaUpdaterTest::CreateSchema(
    absl::Span<const std::string> statements
    ,
    const database_api::DatabaseDialect& dialect,
    bool use_gsql_to_pg_translation) {
  return UpdateSchema(/*base_schema=*/nullptr,
                      statements
                      ,
                      dialect, use_gsql_to_pg_translation);
}

absl::StatusOr<std::unique_ptr<const Schema>> SchemaUpdaterTest::UpdateSchema(
    const Schema* base_schema,
    absl::Span<const std::string> statements
    ,
    const database_api::DatabaseDialect& dialect,
    bool use_gsql_to_pg_translation) {
  std::vector<std::string> pg_ddl_statements;
  if (dialect == database_api::DatabaseDialect::POSTGRESQL &&
      use_gsql_to_pg_translation) {
    // Translate DDL statements from Spanner to PostgreSQL by parsing Spanner
    // DDL into ddl::DDLStatement first and then using PG schema printer to
    // print it to PG DDL. However, DDL statements with expressions are not
    // supported due to the complex reverse translation for expressions from
    // GSQL to PG .
    ddl::DDLStatement ddl_statement;
    ABSL_CHECK_NE(pg_schema_printer_.get(), nullptr);  // Crash OK
    for (const std::string& statement : statements) {
      ZETASQL_RETURN_IF_ERROR(ddl::ParseDDLStatement(statement, &ddl_statement));
      ZETASQL_ASSIGN_OR_RETURN(
          std::vector<std::string> pg_ddl,
          pg_schema_printer_->PrintDDLStatementForEmulator(ddl_statement));
      pg_ddl_statements.push_back(absl::StrJoin(pg_ddl, " "));
    }
    statements = pg_ddl_statements;
  }
  SchemaUpdater updater;
  SchemaChangeContext context{.type_factory = &type_factory_,
                              .table_id_generator = &table_id_generator_,
                              .column_id_generator = &column_id_generator_};
  return updater.ValidateSchemaFromDDL(
      SchemaChangeOperation{
          .statements = statements,
          .database_dialect = dialect,
      },
      context, base_schema);
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
