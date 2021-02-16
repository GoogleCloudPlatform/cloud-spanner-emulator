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

#ifndef STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_MANAGER_H_
#define STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_MANAGER_H_

#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "common/clock.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Session manager manages the set of active sessions in the emulator.
class SessionManager {
 public:
  explicit SessionManager(Clock* clock) : clock_(clock) {}

  // Creates a session attached to the given database.
  zetasql_base::StatusOr<std::shared_ptr<Session>> CreateSession(
      const Labels& labels, std::shared_ptr<Database> database)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a session with the given URI.
  zetasql_base::StatusOr<std::shared_ptr<Session>> GetSession(
      const std::string& session_uri) ABSL_LOCKS_EXCLUDED(mu_);

  // Deletes a session with the given URI.
  absl::Status DeleteSession(const std::string& session_uri)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Lists sessions attached to the given database URI.
  zetasql_base::StatusOr<std::vector<std::shared_ptr<Session>>> ListSessions(
      const std::string& database_uri) const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // System-wide clock.
  Clock* clock_;

  // Mutex to guard state below.
  mutable absl::Mutex mu_;

  // Counter for session ids.
  int next_session_id_ ABSL_GUARDED_BY(mu_) = 0;

  // Map from session URI to session objects.
  std::map<std::string, std::shared_ptr<Session>> session_map_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_MANAGER_H_
