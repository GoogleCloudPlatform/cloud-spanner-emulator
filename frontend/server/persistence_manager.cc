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

#include "frontend/server/persistence_manager.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "googlesql/base/logging.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/snapshot_loader.h"
#include "backend/storage/snapshot_writer.h"
#include "backend/storage/value_serializer.h"
#include "backend/storage/wal_writer.h"
#include "frontend/entities/database.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

// Creates a directory if it does not already exist.
absl::Status EnsureDirectoryExists(const std::string& path) {
  if (mkdir(path.c_str(), 0755) == 0) {
    return absl::OkStatus();
  }
  if (errno == EEXIST) {
    // Verify the existing path is actually a directory.
    struct stat st;
    if (fstatat(AT_FDCWD, path.c_str(), &st, 0) == 0 && S_ISDIR(st.st_mode)) {
      return absl::OkStatus();
    }
    return absl::FailedPreconditionError(
        absl::StrCat("Path exists but is not a directory: ", path));
  }
  return absl::InternalError(
      absl::StrCat("Failed to create directory: ", path,
                    ", errno: ", strerror(errno)));
}

}  // namespace

PersistenceManager::PersistenceManager(const std::string& data_dir)
    : data_dir_(data_dir) {}

std::unique_ptr<PersistenceManager> PersistenceManager::Create(
    const std::string& data_dir) {
  if (data_dir.empty()) {
    return nullptr;
  }

  auto manager =
      std::unique_ptr<PersistenceManager>(new PersistenceManager(data_dir));

  // Ensure data directory and WAL subdirectory exist.
  auto status = EnsureDirectoryExists(data_dir);
  if (!status.ok()) {
    ABSL_LOG(ERROR) << "Failed to create data directory: " << status;
    return nullptr;
  }

  status = EnsureDirectoryExists(manager->wal_directory());
  if (!status.ok()) {
    ABSL_LOG(ERROR) << "Failed to create WAL directory: " << status;
    return nullptr;
  }

  // Create WAL writer.
  auto wal_writer_or = backend::WalWriter::Create(manager->wal_directory());
  if (!wal_writer_or.ok()) {
    ABSL_LOG(ERROR) << "Failed to create WAL writer: "
                    << wal_writer_or.status();
    return nullptr;
  }
  manager->wal_writer_ = std::move(*wal_writer_or);

  return manager;
}

std::string PersistenceManager::snapshot_path() const {
  return absl::StrCat(data_dir_, "/snapshot.pb");
}

std::string PersistenceManager::wal_directory() const {
  return absl::StrCat(data_dir_, "/wal");
}

absl::Status PersistenceManager::RestoreState(ServerEnv* env) {
  // Step 1: Load snapshot if it exists.
  ABSL_LOG(INFO) << "Loading snapshot from: " << snapshot_path();
  auto snapshot_time_or =
      backend::SnapshotLoader::LoadSnapshot(snapshot_path(), env);
  if (snapshot_time_or.ok()) {
    ABSL_LOG(INFO) << "Snapshot loaded successfully.";
  } else if (absl::IsNotFound(snapshot_time_or.status())) {
    ABSL_LOG(INFO) << "No snapshot found at: " << snapshot_path()
                   << ", starting fresh.";
  } else {
    return absl::Status(
        snapshot_time_or.status().code(),
        absl::StrCat("Failed to load snapshot: ",
                      snapshot_time_or.status().message()));
  }

  // Step 2: Read and replay WAL entries.
  auto wal_records_or = backend::WalWriter::ReadAll(wal_directory());
  if (!wal_records_or.ok()) {
    ABSL_LOG(WARNING) << "Failed to read WAL: "
                      << wal_records_or.status().message()
                      << ". Continuing with snapshot data only."
                      << " Some recent changes may be lost.";
    return absl::OkStatus();
  }

  auto records = std::move(*wal_records_or);
  if (!records.empty()) {
    ABSL_LOG(INFO) << "Replaying " << records.size() << " WAL entries.";

    // Sort by sequence number for deterministic replay order.
    std::sort(records.begin(), records.end(),
              [](const backend::WalRecord& a, const backend::WalRecord& b) {
                return a.sequence_number() < b.sequence_number();
              });

    int metadata_count = 0;
    int schema_count = 0;
    int entry_count = 0;

    for (const auto& record : records) {
      if (record.has_metadata_change()) {
        auto status = ReplayMetadataChange(record.metadata_change(), env);
        if (!status.ok()) {
          ABSL_LOG(ERROR) << "Failed to replay metadata change (seq="
                          << record.sequence_number() << "): " << status;
          return status;
        }
        ++metadata_count;
      } else if (record.has_schema_change()) {
        auto status = ReplaySchemaChange(record.schema_change(), env);
        if (!status.ok()) {
          ABSL_LOG(ERROR) << "Failed to replay schema change (seq="
                          << record.sequence_number() << "): " << status;
          return status;
        }
        ++schema_count;
      } else if (record.has_entry()) {
        auto status = ReplayEntry(record.entry(), env);
        if (!status.ok()) {
          ABSL_LOG(ERROR) << "Failed to replay WAL entry (seq="
                          << record.sequence_number() << "): " << status;
          return status;
        }
        ++entry_count;
      }
    }

    ABSL_LOG(INFO) << "WAL replay complete: " << metadata_count
                   << " metadata changes, " << schema_count
                   << " schema changes, " << entry_count
                   << " data entries replayed.";
  }

  return absl::OkStatus();
}

absl::Status PersistenceManager::ReplayMetadataChange(
    const backend::WalMetadataChange& change, ServerEnv* env) {
  namespace instance_api = google::spanner::admin::instance::v1;

  if (change.has_create_instance()) {
    const auto& pi = change.create_instance();
    instance_api::Instance instance_proto;
    if (!instance_proto.ParseFromString(pi.instance_proto())) {
      return absl::InternalError(
          absl::StrCat("Failed to parse instance proto for: ",
                        pi.instance_uri()));
    }
    // Clear node_count if both node_count and processing_units are set,
    // since CreateInstance rejects that combination.
    if (instance_proto.node_count() > 0 &&
        instance_proto.processing_units() > 0) {
      instance_proto.clear_node_count();
    }
    auto result = env->instance_manager()->CreateInstance(
        pi.instance_uri(), instance_proto);
    if (!result.ok()) {
      ABSL_LOG(WARNING) << "Failed to create instance " << pi.instance_uri()
                        << " during WAL replay: " << result.status();
    }
  } else if (change.has_delete_instance_uri()) {
    env->instance_manager()->DeleteInstance(change.delete_instance_uri());
  } else if (change.has_create_database()) {
    const auto& cd = change.create_database();
    std::vector<std::string> ddl_statements(cd.ddl_statements().begin(),
                                            cd.ddl_statements().end());
    backend::SchemaChangeOperation schema_op;
    schema_op.statements = ddl_statements;
    schema_op.database_dialect =
        static_cast<google::spanner::admin::database::v1::DatabaseDialect>(
            cd.dialect());
    auto result = env->database_manager()->CreateDatabase(
        cd.database_uri(), schema_op);
    if (!result.ok()) {
      return absl::Status(
          result.status().code(),
          absl::StrCat("Failed to create database ", cd.database_uri(),
                        " during WAL replay: ", result.status().message()));
    }
  } else if (change.has_delete_database_uri()) {
    auto status = env->database_manager()->DeleteDatabase(
        change.delete_database_uri());
    if (!status.ok()) {
      ABSL_LOG(WARNING) << "Failed to delete database "
                        << change.delete_database_uri()
                        << " during WAL replay: " << status;
    }
  }
  return absl::OkStatus();
}

absl::Status PersistenceManager::ReplaySchemaChange(
    const backend::WalSchemaChange& change, ServerEnv* env) {
  auto db_or = env->database_manager()->GetDatabase(change.database_uri());
  if (!db_or.ok()) {
    return absl::Status(
        db_or.status().code(),
        absl::StrCat("Failed to find database ", change.database_uri(),
                      " for schema change replay: ",
                      db_or.status().message()));
  }
  auto database = *db_or;

  std::vector<std::string> ddl_statements(change.ddl_statements().begin(),
                                          change.ddl_statements().end());
  backend::SchemaChangeOperation schema_op;
  schema_op.statements = ddl_statements;
  schema_op.database_dialect =
      static_cast<google::spanner::admin::database::v1::DatabaseDialect>(
          change.dialect());

  int num_successful = 0;
  absl::Time commit_timestamp;
  absl::Status backfill_status;
  auto status = database->backend()->UpdateSchema(
      schema_op, &num_successful, &commit_timestamp, &backfill_status);
  if (!status.ok()) {
    return absl::Status(
        status.code(),
        absl::StrCat("Failed to replay schema change for ",
                      change.database_uri(), ": ", status.message()));
  }
  if (!backfill_status.ok()) {
    return absl::Status(
        backfill_status.code(),
        absl::StrCat("Schema backfill failed during replay for ",
                      change.database_uri(), ": ",
                      backfill_status.message()));
  }
  return absl::OkStatus();
}

absl::Status PersistenceManager::ReplayEntry(
    const backend::WalEntry& entry, ServerEnv* env) {
  auto db_or = env->database_manager()->GetDatabase(entry.database_uri());
  if (!db_or.ok()) {
    return absl::Status(
        db_or.status().code(),
        absl::StrCat("Failed to find database ", entry.database_uri(),
                      " for WAL entry replay: ", db_or.status().message()));
  }
  auto database = *db_or;
  auto* storage = database->backend()->storage();
  auto* type_factory = database->backend()->type_factory();
  absl::Time commit_timestamp =
      absl::FromUnixMicros(entry.commit_timestamp_micros());

  for (const auto& mutation : entry.mutations()) {
    if (mutation.has_write()) {
      const auto& write = mutation.write();

      auto key_or = backend::DeserializeKey(write.key(), type_factory);
      if (!key_or.ok()) {
        return absl::Status(
            key_or.status().code(),
            absl::StrCat("Failed to deserialize key for table ",
                          write.table_id(), ": ", key_or.status().message()));
      }

      std::vector<std::string> column_ids(write.column_ids().begin(),
                                          write.column_ids().end());
      std::vector<zetasql::Value> values;
      values.reserve(write.values_size());
      for (const auto& pv : write.values()) {
        auto value_or = backend::DeserializeValue(pv, type_factory);
        if (!value_or.ok()) {
          return absl::Status(
              value_or.status().code(),
              absl::StrCat("Failed to deserialize value for table ",
                            write.table_id(), ": ",
                            value_or.status().message()));
        }
        values.push_back(std::move(*value_or));
      }

      auto status = storage->Write(commit_timestamp, write.table_id(),
                                   *key_or, column_ids, values);
      if (!status.ok()) {
        return absl::Status(
            status.code(),
            absl::StrCat("Failed to write to table ", write.table_id(),
                          " during WAL replay: ", status.message()));
      }
    } else if (mutation.has_delete_op()) {
      const auto& del = mutation.delete_op();

      auto start_key_or =
          backend::DeserializeKey(del.start_key(), type_factory);
      if (!start_key_or.ok()) {
        return absl::Status(
            start_key_or.status().code(),
            absl::StrCat("Failed to deserialize start key for table ",
                          del.table_id(), ": ",
                          start_key_or.status().message()));
      }

      auto end_key_or = backend::DeserializeKey(del.end_key(), type_factory);
      if (!end_key_or.ok()) {
        return absl::Status(
            end_key_or.status().code(),
            absl::StrCat("Failed to deserialize end key for table ",
                          del.table_id(), ": ",
                          end_key_or.status().message()));
      }

      auto key_range = backend::KeyRange::ClosedOpen(
          *start_key_or, *end_key_or);
      auto status = storage->Delete(commit_timestamp, del.table_id(),
                                    key_range);
      if (!status.ok()) {
        return absl::Status(
            status.code(),
            absl::StrCat("Failed to delete from table ", del.table_id(),
                          " during WAL replay: ", status.message()));
      }
    }
  }
  return absl::OkStatus();
}

absl::Status PersistenceManager::SaveState(ServerEnv* env) {
  // Step 1: Write snapshot.
  ABSL_LOG(INFO) << "Writing snapshot to: " << snapshot_path();
  auto status = backend::SnapshotWriter::WriteSnapshot(
      snapshot_path(), env->instance_manager(), env->database_manager());
  if (!status.ok()) {
    return absl::Status(
        status.code(),
        absl::StrCat("Failed to write snapshot: ", status.message()));
  }
  ABSL_LOG(INFO) << "Snapshot written successfully.";

  // Step 2: Sync and clear WAL.
  status = wal_writer_->Sync();
  if (!status.ok()) {
    return absl::Status(
        status.code(),
        absl::StrCat("Failed to sync WAL: ", status.message()));
  }

  status = backend::WalWriter::Clear(wal_directory());
  if (!status.ok()) {
    return absl::Status(
        status.code(),
        absl::StrCat("Failed to clear WAL: ", status.message()));
  }
  ABSL_LOG(INFO) << "WAL cleared after snapshot.";

  return absl::OkStatus();
}

void PersistenceManager::StartPeriodicSnapshots(ServerEnv* env,
                                                 absl::Duration interval) {
  if (interval <= absl::ZeroDuration()) {
    return;
  }
  snapshot_thread_ = std::thread(&PersistenceManager::SnapshotLoop, this, env,
                                 interval);
}

void PersistenceManager::StopPeriodicSnapshots() {
  {
    absl::MutexLock lock(&snapshot_mu_);
    snapshot_stop_ = true;
  }
  if (snapshot_thread_.joinable()) {
    snapshot_thread_.join();
  }
}

void PersistenceManager::SnapshotLoop(ServerEnv* env,
                                       absl::Duration interval) {
  while (true) {
    {
      absl::MutexLock lock(&snapshot_mu_);
      auto stop = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(snapshot_mu_) {
        return snapshot_stop_;
      };
      if (snapshot_mu_.AwaitWithTimeout(absl::Condition(&stop), interval)) {
        // Stop was requested.
        return;
      }
    }
    // Interval elapsed — take a snapshot.
    ABSL_LOG(INFO) << "Periodic snapshot starting.";
    auto status = SaveState(env);
    if (!status.ok()) {
      ABSL_LOG(ERROR) << "Periodic snapshot failed: " << status;
    } else {
      ABSL_LOG(INFO) << "Periodic snapshot completed.";
    }
  }
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
