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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCHEMA_UPDATER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCHEMA_UPDATER_H_

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/common/ids.h"
#include "backend/database/pg_oid_assigner/pg_oid_assigner.h"
#include "backend/schema/catalog/schema.h"

// If true, disable to limit on change stream retention period between 1h and
// 7day to prevent long running integration tests.
ABSL_DECLARE_FLAG(bool, cloud_spanner_emulator_disable_cs_retention_check);

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
static constexpr char kIndexDataTablePrefix[] = "_index_data_table_";
// Container holding all the required inputs for processing a schema change.
struct SchemaChangeOperation {
  absl::Span<const std::string> statements;
  absl::string_view proto_descriptor_bytes;
  ::google::spanner::admin::database::v1::DatabaseDialect database_dialect =
      ::google::spanner::admin::database::v1::GOOGLE_STANDARD_SQL;
};

// Database context within which a schema change is processed.
struct SchemaChangeContext {
  // Type factory for the database.
  zetasql::TypeFactory* type_factory;

  // Unique table ID generator for the database.
  TableIDGenerator* table_id_generator;

  // Unique column ID generator for the database.
  ColumnIDGenerator* column_id_generator;

  // The database's storage, for data-dependent validations/backfills.
  Storage* storage;

  // The timestamp at which the schema changes/validations/backfills
  // should be done.
  absl::Time schema_change_timestamp;

  // Assigns OIDs to database objects when dialect is POSTGRESQL. The assigner
  // is owned by the database and is shared across all schema changes.
  PgOidAssigner* pg_oid_assigner;
};

// The result of processing a set of DDL statements for a schema change request.
struct SchemaChangeResult {
  // The number of successfully applied DDL statements.
  int num_successful_statements;

  // The schema snapshot resulting from the last successfully applied
  // DDL statement.
  std::unique_ptr<const Schema> updated_schema;

  // The error encounterd while processing the first backfill/verifier action
  // that failed. absl::OkStatus() if all schema actions successfully applied.
  absl::Status backfill_status;
};

// Parses the given statement based on the dialect and returns the DDL
// statement.
absl::StatusOr<std::unique_ptr<ddl::DDLStatement>> ParseDDLByDialect(
    absl::string_view statement, database_api::DatabaseDialect dialect);

class SchemaUpdater {
 public:
  SchemaUpdater() = default;

  // Creates a new Schema from `schema_change_operation.statements` or returns
  // the error encountered while applying the first invalid statement. Also runs
  // any backfill or data-dependent verification tasks resulting from the new
  // schema such as creation of a new index. However, since the database will
  // not contain any data at this point, none of the backfill tasks are expected
  // to fail.
  absl::StatusOr<std::unique_ptr<const Schema>> CreateSchemaFromDDL(
      const SchemaChangeOperation& schema_change_operation,
      const SchemaChangeContext& context);

  // Applies the DDL statements in `schema_change_operation.statements` on top
  // of `existing_schema`. Any errors during semantic validation of the provided
  // `statements` are communicated through the return status of the function.
  //
  // If the set of statements is semantically valid, but results in a schema
  // verification/backfill error, then that is communicated through the returned
  // `SchemaChangeResult`'s backfill_status member with the schema snapshot
  // corresponding to the last succesfully applied statement and the number of
  // successfully applied statements returned in the `updated_schema` and
  // `num_successful_statements` members respectively.
  absl::StatusOr<SchemaChangeResult> UpdateSchemaFromDDL(
      const Schema* existing_schema,
      const SchemaChangeOperation& schema_change_operation,
      const SchemaChangeContext& context);

  // Validates the given set DDL statements, producing a new schema with the
  // DDL statements applied. Does not run any backfill/verification tasks
  // entailed by `statements`.
  absl::StatusOr<std::unique_ptr<const Schema>> ValidateSchemaFromDDL(
      const SchemaChangeOperation& schema_change_operation,
      const SchemaChangeContext& context,
      const Schema* existing_schema = nullptr);

 private:
  absl::Status RunPendingActions(int* num_succesful);

  std::vector<SchemaValidationContext> pending_work_;

  std::vector<std::unique_ptr<const Schema>> intermediate_schemas_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCHEMA_UPDATER_H_
