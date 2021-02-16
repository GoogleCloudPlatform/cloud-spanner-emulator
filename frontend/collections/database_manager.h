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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_DATABASE_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_DATABASE_MANAGER_H_

#include <map>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "common/clock.h"
#include "frontend/entities/database.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// DatabaseManager manages the set of active databases in the emulator.
class DatabaseManager {
 public:
  explicit DatabaseManager(Clock* clock) : clock_(clock) {}

  // Creates a database with a schema initialized from `create_statements`.
  zetasql_base::StatusOr<std::shared_ptr<Database>> CreateDatabase(
      const std::string& database_uri,
      const std::vector<std::string>& create_statements)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a database with the given URI.
  zetasql_base::StatusOr<std::shared_ptr<Database>> GetDatabase(
      const std::string& database_uri) const ABSL_LOCKS_EXCLUDED(mu_);

  // Deletes a database with the given URI.
  absl::Status DeleteDatabase(const std::string& database_uri)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Lists all databases associated with the given instance URI.
  zetasql_base::StatusOr<std::vector<std::shared_ptr<Database>>> ListDatabases(
      const std::string& instance_uri) const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // System-wide clock.
  Clock* clock_;

  // Mutex to guard state below.
  mutable absl::Mutex mu_;

  // Map from database URI to database objects.
  std::map<std::string, std::shared_ptr<Database>> database_map_
      ABSL_GUARDED_BY(mu_);

  // Count of databases per instance.
  absl::flat_hash_map<std::string, int> num_databases_per_instance_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_DATABASE_MANAGER_H_
