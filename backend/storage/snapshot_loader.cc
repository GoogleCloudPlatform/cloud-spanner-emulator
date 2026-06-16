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

#include "backend/storage/snapshot_loader.h"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "backend/access/write.h"
#include "backend/common/ids.h"
#include "backend/database/database.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/value_serializer.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "frontend/collections/database_manager.h"
#include "frontend/collections/instance_manager.h"
#include "frontend/entities/database.h"
#include "frontend/server/environment.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

// Populate the storage of a database from persisted data.
//
// We use a ReadWriteTransaction to write the data via Mutations, which
// handles constraint checks and index maintenance correctly.
absl::Status PopulateStorage(
    backend::Database* database,
    const PersistedStorage& storage_proto,
    absl::Time restore_timestamp) {
  const Schema* schema = database->GetLatestSchema();
  if (schema == nullptr) {
    return absl::InternalError("Database has no schema after creation");
  }

  // Use the database's TypeFactory for value deserialization.
  zetasql::TypeFactory* type_factory = database->type_factory();

  // Create a read-write transaction to write data.
  ReadWriteOptions rw_options;
  RetryState retry_state;
  ZETASQL_ASSIGN_OR_RETURN(auto txn,
                   database->CreateReadWriteTransaction(rw_options,
                                                        retry_state));

  // Build a single Mutation containing all insert operations.
  Mutation mutation;

  for (const auto& table_proto : storage_proto.tables()) {
    // Find the table in the schema by table_id.
    const Table* table = nullptr;
    for (const auto* t : schema->tables()) {
      if (t->id() == table_proto.table_id()) {
        table = t;
        break;
      }
    }
    if (table == nullptr) {
      LOG(WARNING) << "Table with ID " << table_proto.table_id()
                   << " not found in schema, skipping data restore.";
      continue;
    }

    // Build a column_id -> Column* mapping for this table.
    absl::flat_hash_map<ColumnID, const Column*> column_map;
    for (const auto* col : table->columns()) {
      column_map[col->id()] = col;
    }

    for (const auto& row_proto : table_proto.rows()) {
      // For each row, collect column names and values from the persisted cells.
      std::vector<std::string> col_names;
      std::vector<zetasql::Value> col_values;

      for (const auto& cell_proto : row_proto.cells()) {
        auto it = column_map.find(cell_proto.column_id());
        if (it == column_map.end()) {
          LOG(WARNING) << "Column ID " << cell_proto.column_id()
                       << " not found in table " << table->Name()
                       << ", skipping.";
          continue;
        }
        const Column* col = it->second;

        // Use the latest version of the value.
        if (cell_proto.versions_size() == 0) {
          continue;
        }
        const auto& latest_version =
            cell_proto.versions(cell_proto.versions_size() - 1);

        ZETASQL_ASSIGN_OR_RETURN(
            auto value,
            DeserializeValue(latest_version.value(), type_factory));

        col_names.push_back(col->Name());
        col_values.push_back(std::move(value));
      }

      if (col_names.empty()) {
        continue;
      }

      // Add an Insert mutation for this row.
      std::vector<ValueList> rows;
      rows.push_back(std::move(col_values));
      mutation.AddWriteOp(MutationOpType::kInsert, table->Name(),
                          std::move(col_names), std::move(rows));
    }
  }

  // Write the mutation and commit the transaction.
  ZETASQL_RETURN_IF_ERROR(txn->Write(mutation));
  ZETASQL_RETURN_IF_ERROR(txn->Commit());

  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<absl::Time> SnapshotLoader::LoadSnapshot(
    const std::string& snapshot_path,
    frontend::ServerEnv* env) {
  // Read the snapshot proto from disk.
  EmulatorSnapshot snapshot;
  {
    std::ifstream in(snapshot_path, std::ios::binary);
    if (!in.is_open()) {
      return absl::NotFoundError(
          absl::StrCat("Snapshot file not found: ", snapshot_path));
    }
    if (!snapshot.ParseFromIstream(&in)) {
      return absl::InternalError(
          absl::StrCat("Failed to parse snapshot from: ", snapshot_path));
    }
  }

  absl::Time snapshot_time =
      absl::FromUnixMicros(snapshot.snapshot_timestamp_micros());

  LOG(INFO) << "Loading snapshot from " << snapshot_path << " ("
            << snapshot.instances_size() << " instances, "
            << snapshot.databases_size() << " databases, timestamp="
            << snapshot_time << ")";

  // Restore instances.
  for (const auto& pi : snapshot.instances()) {
    instance_api::Instance instance_proto;
    if (!instance_proto.ParseFromString(pi.instance_proto())) {
      return absl::InternalError(
          absl::StrCat("Failed to parse instance proto for: ",
                        pi.instance_uri()));
    }

    // The serialized proto may have both node_count and processing_units set
    // (Instance::ToProto sets both), but CreateInstance rejects that. Clear
    // node_count since processing_units is the canonical representation.
    if (instance_proto.node_count() > 0 &&
        instance_proto.processing_units() > 0) {
      instance_proto.clear_node_count();
    }

    auto result = env->instance_manager()->CreateInstance(
        pi.instance_uri(), instance_proto);
    if (!result.ok()) {
      LOG(WARNING) << "Failed to create instance " << pi.instance_uri()
                   << " during restore: " << result.status();
      // Continue anyway -- the instance might already exist.
    }
  }

  // Restore databases.
  for (const auto& pd : snapshot.databases()) {
    // Build SchemaChangeOperation from DDL statements.
    // SchemaChangeOperation.statements is absl::Span<const std::string>,
    // so we need to keep the vector alive while CreateDatabase runs.
    std::vector<std::string> ddl_statements(pd.ddl_statements().begin(),
                                            pd.ddl_statements().end());
    SchemaChangeOperation schema_op;
    schema_op.statements = ddl_statements;
    schema_op.database_dialect =
        static_cast<google::spanner::admin::database::v1::DatabaseDialect>(
            pd.dialect());

    ZETASQL_ASSIGN_OR_RETURN(
        auto database,
        env->database_manager()->CreateDatabase(pd.database_uri(), schema_op));

    // Populate storage data if present.
    if (pd.has_storage() && pd.storage().tables_size() > 0) {
      // Use a timestamp slightly before the snapshot time for data writes,
      // so that reads at snapshot_time will see the data.
      absl::Time data_timestamp =
          snapshot_time - absl::Microseconds(1);

      ZETASQL_RETURN_IF_ERROR(
          PopulateStorage(database->backend(), pd.storage(), data_timestamp));
    }

    // Restore ID generator sequence numbers if present.
    if (pd.next_table_id_seq() > 0) {
      database->backend()->table_id_generator().SetCurrentValue(pd.next_table_id_seq());
    }
    if (pd.next_column_id_seq() > 0) {
      database->backend()->column_id_generator().SetCurrentValue(pd.next_column_id_seq());
    }
    if (pd.next_change_stream_id_seq() > 0) {
      database->backend()->change_stream_id_generator().SetCurrentValue(pd.next_change_stream_id_seq());
    }
    if (pd.next_transaction_id_seq() > 0) {
      database->backend()->transaction_id_generator().SetCurrentValue(pd.next_transaction_id_seq());
    }

    LOG(INFO) << "Restored database " << pd.database_uri() << " with "
              << ddl_statements.size() << " DDL statements and "
              << pd.storage().tables_size() << " tables of data.";
  }

  LOG(INFO) << "Snapshot load complete.";
  return snapshot_time;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
