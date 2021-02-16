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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_DATABASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_DATABASE_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "backend/actions/manager.h"
#include "backend/common/ids.h"
#include "backend/locking/manager.h"
#include "backend/query/query_engine.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/storage/storage.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Database represents a database in the emulator backend.
//
// Database largely ties together various subsystems - transactions, locking,
// schemas, queries, storage etc. and acts as a container for these subsystems.
class Database {
 public:
  // Constructs a fully initialized database with schema created using
  // create_statements. Returns an error if create_statements are invalid, or if
  // failed to create the database.
  static zetasql_base::StatusOr<std::unique_ptr<Database>> Create(
      Clock* clock, const std::vector<std::string>& create_statements);

  // Creates a read only transaction attached to this database.
  zetasql_base::StatusOr<std::unique_ptr<ReadOnlyTransaction>>
  CreateReadOnlyTransaction(const ReadOnlyOptions& options);

  // Creates a read write transaction attached to this database.
  zetasql_base::StatusOr<std::unique_ptr<ReadWriteTransaction>>
  CreateReadWriteTransaction(const ReadWriteOptions& options,
                             const RetryState& retry_state);

  // Updates the schema for this database.
  //
  // All schema changes are applied synchronously and transactionally.
  // If there are any transactions or other schema changes already in progress,
  // incoming schema change requests will be rejected with a FAILED_PRECONDITION
  // error.
  //
  // DDL statements in `statements` are applied one-by-one until they either all
  // succeed or the first failure is encoutered.
  //
  // On return `num_successful_statements` will contain the number of
  // successfully applied DDL statements and `commit_timestamp` will contain the
  // timestamp at which they were applied.
  //
  // If all the statements in `statements` are applied succesfully, both
  // `backfill_status` and the returned status will be set to
  // absl::OkStatus().
  //
  // If all the statements are semantically valid then the return status will
  // be absl::OkStatus(). Otherwise, a non-ok absl::Status will be returned for
  // the first statement that is found to be invalid. In case of a non-ok
  // return status the output parameters (including the `backfill_status`)
  // may not be set.
  //
  // If all the statements are found to be semantically valid, but an error is
  // encountered while processing the backfill/verification actions for the
  // statements, then the first such error will be returned in
  // `backfill_status`.
  absl::Status UpdateSchema(absl::Span<const std::string> statements,
                            int* num_succesful_statements,
                            absl::Time* commit_timestamp,
                            absl::Status* backfill_status);

  // Retrives the current version of the schema.
  const Schema* GetLatestSchema() const;

  // Used to execute queries against the database.
  QueryEngine* query_engine() { return query_engine_.get(); }

 private:
  Database();
  // Delete copy and assignment operators since database shouldn't be copyable.
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  SchemaChangeContext GetSchemaChangeContext();

  // Clock to provide commit timestamps.
  Clock* clock_;

  // Unique ID generator for TransactionID.
  TransactionIDGenerator transaction_id_generator_;

  // Unique ID generator for storage TableIDs.
  TableIDGenerator table_id_generator_;

  // Unique ID generator for storage ColumnIDs.
  ColumnIDGenerator column_id_generator_;

  // Underlying storage for the database.
  std::unique_ptr<Storage> storage_;

  // Lock management.
  std::unique_ptr<LockManager> lock_manager_;

  // Type factory used for all ZetaSQL operations on this database.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;

  // Versioned catalog of this database.
  std::unique_ptr<VersionedCatalog> versioned_catalog_;

  // Query engine of the database.
  std::unique_ptr<QueryEngine> query_engine_;

  // Maintains an action registry per schema.
  std::unique_ptr<ActionManager> action_manager_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_DATABASE_H_
