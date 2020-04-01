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

#include "frontend/collections/database_manager.h"

#include <map>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "backend/database/database.h"
#include "common/clock.h"
#include "common/errors.h"
#include "common/limits.h"
#include "frontend/common/uris.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

std::vector<std::shared_ptr<Database>> GetDatabasesByInstance(
    const std::map<std::string, std::shared_ptr<Database>>& database_map,
    const std::string& instance_uri) {
  std::string database_uri_prefix = absl::StrCat(instance_uri, "/");
  std::vector<std::shared_ptr<Database>> databases;
  auto itr = database_map.upper_bound(database_uri_prefix);
  while (itr != database_map.end()) {
    if (!absl::StartsWith(itr->first, database_uri_prefix)) {
      break;
    }
    databases.push_back(itr->second);
    ++itr;
  }
  return databases;
}

}  // namespace

zetasql_base::StatusOr<std::shared_ptr<Database>> DatabaseManager::CreateDatabase(
    const std::string& database_uri,
    const std::vector<std::string>& create_statements) {
  absl::MutexLock lock(&mu_);
  auto itr = database_map_.find(database_uri);
  if (itr != database_map_.end()) {
    return error::DatabaseAlreadyExists(database_uri);
  }

  absl::string_view project_id, instance_id, database_id;
  ZETASQL_RETURN_IF_ERROR(
      ParseDatabaseUri(database_uri, &project_id, &instance_id, &database_id));
  std::string instance_uri = MakeInstanceUri(project_id, instance_id);
  if (GetDatabasesByInstance(database_map_, instance_uri).size() >=
      limits::kMaxDatabasesPerInstance) {
    return error::TooManyDatabasesPerInstance(instance_uri);
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<backend::Database> backend_db,
                   backend::Database::Create(clock_, create_statements));
  database_map_[database_uri] = std::make_shared<Database>(
      database_uri, std::move(backend_db), clock_->Now());
  return database_map_.find(database_uri)->second;
}

zetasql_base::StatusOr<std::shared_ptr<Database>> DatabaseManager::GetDatabase(
    const std::string& database_uri) const {
  absl::MutexLock lock(&mu_);
  auto itr = database_map_.find(database_uri);
  if (itr == database_map_.end()) {
    return error::DatabaseNotFound(database_uri);
  }
  return itr->second;
}

zetasql_base::Status DatabaseManager::DeleteDatabase(const std::string& database_uri) {
  absl::MutexLock lock(&mu_);
  database_map_.erase(database_uri);
  return zetasql_base::OkStatus();
}

zetasql_base::StatusOr<std::vector<std::shared_ptr<Database>>>
DatabaseManager::ListDatabases(const std::string& instance_uri) const {
  absl::MutexLock lock(&mu_);
  return GetDatabasesByInstance(database_map_, instance_uri);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
