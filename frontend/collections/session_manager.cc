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

#include "frontend/collections/session_manager.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "common/errors.h"
#include "frontend/common/labels.h"
#include "frontend/common/uris.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

absl::StatusOr<std::shared_ptr<Session>> SessionManager::CreateSession(
    const Labels& labels, const bool multiplexed,
    std::shared_ptr<Database> database) {
  absl::MutexLock lock(&mu_);
  const std::string session_id = absl::StrCat(next_session_id_++);
  std::string session_uri =
      MakeSessionUri(database->database_uri(), session_id);
  std::shared_ptr<Session> session =
      std::make_shared<Session>(session_uri, labels, multiplexed,
                                /* create_time = */ clock_->Now(), database);
  session->set_approximate_last_use_time(clock_->Now());

  session_map_[session_uri] = session;
  return session;
}

absl::StatusOr<std::shared_ptr<Session>> SessionManager::GetSession(
    const std::string& session_uri) {
  absl::MutexLock lock(&mu_);
  auto itr = session_map_.find(session_uri);
  if (itr == session_map_.end()) {
    return error::SessionNotFound(session_uri);
  }
  std::shared_ptr<Session> session = itr->second;
  absl::Duration expiration_duration =
      session->multiplexed() ? absl::Hours(28 * 24) : absl::Hours(1);
  if (clock_->Now() - session->approximate_last_use_time() >
      expiration_duration) {
    // Delete inactive sessions after expiration duration.
    session_map_.erase(session_uri);
    return error::SessionNotFound(session_uri);
  }
  session->set_approximate_last_use_time(clock_->Now());
  return session;
}

absl::StatusOr<std::vector<std::shared_ptr<Session>>>
SessionManager::ListSessions(const std::string& database_uri) const {
  absl::MutexLock lock(&mu_);
  std::string session_uri_prefix = absl::StrCat(database_uri, "/");
  std::vector<std::shared_ptr<Session>> sessions;
  for (auto itr = session_map_.lower_bound(session_uri_prefix);
       itr != session_map_.end(); ++itr) {
    if (absl::StartsWith(itr->first, session_uri_prefix)) {
      std::shared_ptr<Session> session = itr->second;
      absl::Duration expiration_duration =
          session->multiplexed() ? absl::Hours(28 * 24) : absl::Hours(1);
      if (clock_->Now() - session->approximate_last_use_time() <=
          expiration_duration) {
        sessions.push_back(session);
      }
    } else {
      return sessions;
    }
  }
  return sessions;
}

absl::Status SessionManager::DeleteSession(const std::string& session_uri) {
  absl::MutexLock lock(&mu_);
  session_map_.erase(session_uri);
  return absl::OkStatus();
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
