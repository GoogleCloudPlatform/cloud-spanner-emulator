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

#include "backend/storage/snapshot_writer.h"

#include <cstdio>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/datamodel/key_set.h"
#include "backend/database/database.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/printer/print_ddl.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/value_serializer.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "frontend/collections/database_manager.h"
#include "frontend/collections/instance_manager.h"
#include "frontend/entities/database.h"
#include "frontend/entities/instance.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

// Extract the database ID (just the name part) from a database URI.
// Database URI format: projects/<project>/instances/<instance>/databases/<db>
std::string ExtractDatabaseId(const std::string& database_uri) {
  auto pos = database_uri.rfind('/');
  if (pos == std::string::npos) {
    return database_uri;
  }
  return database_uri.substr(pos + 1);
}

}  // namespace

absl::StatusOr<PersistedStorage> SnapshotWriter::SerializeStorage(
    backend::Database* database, absl::Time read_timestamp) {
  PersistedStorage storage_proto;

  const Schema* schema = database->GetLatestSchema();
  if (schema == nullptr) {
    return storage_proto;
  }

  // Create a read-only transaction with a strong read to get the latest data.
  // We use kStrongRead instead of kExactTimestamp because the provided
  // read_timestamp may not align with committed data timestamps.
  ReadOnlyOptions read_options;
  read_options.bound = TimestampBound::kStrongRead;
  ZETASQL_ASSIGN_OR_RETURN(auto txn,
                   database->CreateReadOnlyTransaction(read_options));

  // Iterate all tables in the schema.
  for (const auto* table : schema->tables()) {
    // Skip internal tables (index backing tables, change stream tables).
    if (table->owner_index() != nullptr ||
        table->owner_change_stream() != nullptr) {
      continue;
    }

    PersistedTable* table_proto = storage_proto.add_tables();
    table_proto->set_table_id(table->id());

    // Build list of column names and IDs for this table.
    std::vector<std::string> column_names;
    std::vector<ColumnID> column_ids;
    for (const auto* col : table->columns()) {
      column_names.push_back(col->Name());
      column_ids.push_back(col->id());
    }

    // Read all rows from this table.
    ReadArg read_arg;
    read_arg.table = table->Name();
    read_arg.key_set = KeySet::All();
    read_arg.columns = column_names;

    std::unique_ptr<RowCursor> cursor;
    ZETASQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

    while (cursor->Next()) {
      PersistedRow* row_proto = table_proto->add_rows();

      // Serialize the key. The key columns are the primary key columns of the
      // table. We serialize all column values as a flat row; on restore we
      // rebuild from DDL + data.
      // For the key, we use the primary key column values from the cursor.
      PersistedKey* key_proto = row_proto->mutable_key();

      // Build the key from primary key columns.
      const auto& pk_columns = table->primary_key();
      for (const auto* pk_col : pk_columns) {
        const std::string& pk_name = pk_col->column()->Name();
        // Find the index of this column in our column list.
        for (int i = 0; i < static_cast<int>(column_names.size()); ++i) {
          if (column_names[i] == pk_name) {
            zetasql::Value val = cursor->ColumnValue(i);
            ZETASQL_ASSIGN_OR_RETURN(auto persisted_val, SerializeValue(val));
            PersistedKeyColumn* key_col = key_proto->add_columns();
            *key_col->mutable_value() = std::move(persisted_val);
            key_col->set_is_descending(pk_col->is_descending());
            break;
          }
        }
      }

      // Serialize all column values (including key columns, for simplicity).
      for (int i = 0; i < static_cast<int>(column_ids.size()); ++i) {
        zetasql::Value val = cursor->ColumnValue(i);
        if (!val.is_valid()) {
          // Skip invalid/unset values.
          continue;
        }
        PersistedCell* cell_proto = row_proto->add_cells();
        cell_proto->set_column_id(column_ids[i]);

        // We only have one version (the current one at read_timestamp).
        TimestampedValue* tv = cell_proto->add_versions();
        tv->set_timestamp_micros(absl::ToUnixMicros(read_timestamp));
        ZETASQL_ASSIGN_OR_RETURN(auto persisted_val, SerializeValue(val));
        *tv->mutable_value() = std::move(persisted_val);
      }
    }
    ZETASQL_RETURN_IF_ERROR(cursor->Status());
  }

  return storage_proto;
}

absl::Status SnapshotWriter::WriteSnapshot(
    const std::string& snapshot_path,
    frontend::InstanceManager* instance_manager,
    frontend::DatabaseManager* database_manager) {
  EmulatorSnapshot snapshot;
  absl::Time now = absl::Now();
  snapshot.set_snapshot_timestamp_micros(absl::ToUnixMicros(now));

  // Iterate all instances across all projects.
  auto instances = instance_manager->ListAllInstances();

  // Collect all unique project URIs from instance URIs for later use.
  std::set<std::string> instance_uris;
  for (const auto& instance : instances) {
    instance_uris.insert(instance->instance_uri());

    // Serialize the instance.
    PersistedInstance* pi = snapshot.add_instances();
    pi->set_instance_uri(instance->instance_uri());

    instance_api::Instance instance_proto;
    instance->ToProto(&instance_proto);
    pi->set_instance_proto(instance_proto.SerializeAsString());
  }

  // For each instance, list and serialize all its databases.
  for (const auto& inst_uri : instance_uris) {
    ZETASQL_ASSIGN_OR_RETURN(auto databases,
                     database_manager->ListDatabases(inst_uri));

    for (const auto& db : databases) {
      PersistedDatabase* pd = snapshot.add_databases();
      pd->set_database_uri(db->database_uri());
      pd->set_database_id(ExtractDatabaseId(db->database_uri()));

      backend::Database* backend = db->backend();
      pd->set_dialect(static_cast<int32_t>(backend->dialect()));

      // Extract DDL statements from the current schema using PrintDDLStatements.
      const Schema* schema = backend->GetLatestSchema();
      if (schema != nullptr) {
        auto ddl_result = PrintDDLStatements(schema);
        if (ddl_result.ok()) {
          for (const auto& stmt : ddl_result.value()) {
            pd->add_ddl_statements(stmt);
          }
        } else {
          LOG(WARNING) << "Failed to print DDL for database "
                       << db->database_uri() << ": " << ddl_result.status();
          // Continue without DDL -- the database will be empty on restore.
        }
      }

      // Serialize storage data.
      // We read at the current time so we get the latest committed data.
      auto storage_result = SerializeStorage(backend, now);
      if (storage_result.ok()) {
        *pd->mutable_storage() = std::move(storage_result.value());
      } else {
        LOG(WARNING) << "Failed to serialize storage for database "
                     << db->database_uri() << ": " << storage_result.status();
      }

      // Persist ID generator sequence numbers so they can be restored.
      pd->set_next_table_id_seq(backend->table_id_generator().GetCurrentValue());
      pd->set_next_column_id_seq(backend->column_id_generator().GetCurrentValue());
      pd->set_next_change_stream_id_seq(backend->change_stream_id_generator().GetCurrentValue());
      pd->set_next_transaction_id_seq(backend->transaction_id_generator().GetCurrentValue());
    }
  }

  // Write atomically: write to .tmp, then rename.
  std::string tmp_path = snapshot_path + ".tmp";
  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return absl::InternalError(
          absl::StrCat("Failed to open snapshot file for writing: ", tmp_path));
    }
    if (!snapshot.SerializeToOstream(&out)) {
      return absl::InternalError(
          absl::StrCat("Failed to serialize snapshot to: ", tmp_path));
    }
    out.close();
    if (out.fail()) {
      return absl::InternalError(
          absl::StrCat("Failed to write snapshot to: ", tmp_path));
    }
  }

  if (std::rename(tmp_path.c_str(), snapshot_path.c_str()) != 0) {
    return absl::InternalError(
        absl::StrCat("Failed to rename snapshot file from ", tmp_path, " to ",
                      snapshot_path));
  }

  LOG(INFO) << "Snapshot written to " << snapshot_path << " ("
            << snapshot.instances_size() << " instances, "
            << snapshot.databases_size() << " databases)";
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
