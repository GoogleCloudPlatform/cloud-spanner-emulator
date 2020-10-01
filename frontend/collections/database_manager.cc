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
#include "zetasql/base/statusor.h"
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
  // Perform bulk of the work outside the database manager lock to allow
  // CreateDatabase calls to execute in parallel. A common test pattern is to
  // Create/Drop a database per unit test, and run unit tests in parallel. So
  // we want to optimize this use case.
  absl::string_view project_id, instance_id, database_id;
  ZETASQL_RETURN_IF_ERROR(
      ParseDatabaseUri(database_uri, &project_id, &instance_id, &database_id));
  std::string instance_uri = MakeInstanceUri(project_id, instance_id);

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<backend::Database> backend_db,
                   backend::Database::Create(clock_, create_statements));
  auto database = std::make_shared<Database>(
      database_uri, std::move(backend_db), clock_->Now());

  // Now update the database manager state. We could do the validation checks
  // at the top of this function, but we would have to do it here again anyway,
  // so we don't bother optimizing that case.
  absl::MutexLock lock(&mu_);

  // Check that a database with this name does not already exist.
  auto itr = database_map_.find(database_uri);
  if (itr != database_map_.end()) {
    return error::DatabaseAlreadyExists(database_uri);
  }

  // Check that the user did not exceed their database quota.
  if (num_databases_per_instance_[instance_uri] >=
      limits::kMaxDatabasesPerInstance) {
    return error::TooManyDatabasesPerInstance(instance_uri);
  }

  // Record this database in the database manager.
  database_map_[database_uri] = database;
  num_databases_per_instance_[instance_uri] += 1;

  return database;
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

absl::Status DatabaseManager::DeleteDatabase(const std::string& database_uri) {
  absl::MutexLock lock(&mu_);
  if (database_map_.erase(database_uri) > 0) {
    absl::string_view project_id, instance_id, database_id;
    ZETASQL_RETURN_IF_ERROR(ParseDatabaseUri(database_uri, &project_id, &instance_id,
                                     &database_id));
    std::string instance_uri = MakeInstanceUri(project_id, instance_id);
    num_databases_per_instance_[instance_uri] -= 1;
  }
  return absl::OkStatus();
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
