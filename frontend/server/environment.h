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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_ENV_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_ENV_H_

#include <memory>

#include "common/clock.h"
#include "frontend/collections/database_manager.h"
#include "frontend/collections/instance_manager.h"
#include "frontend/collections/operation_manager.h"
#include "frontend/collections/session_manager.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// ServerEnv encapsulates global objects for Cloud Spanner Emulator.
class ServerEnv {
 public:
  ServerEnv()
      : clock_(new Clock()),
        database_manager_(new DatabaseManager(clock_.get())),
        instance_manager_(new InstanceManager()),
        operation_manager_(new OperationManager()),
        session_manager_(new SessionManager(clock_.get())) {}

  Clock* clock() { return clock_.get(); }
  DatabaseManager* database_manager() { return database_manager_.get(); }
  InstanceManager* instance_manager() { return instance_manager_.get(); }
  OperationManager* operation_manager() { return operation_manager_.get(); }
  SessionManager* session_manager() { return session_manager_.get(); }

 private:
  std::unique_ptr<Clock> clock_;
  std::unique_ptr<DatabaseManager> database_manager_;
  std::unique_ptr<InstanceManager> instance_manager_;
  std::unique_ptr<OperationManager> operation_manager_;
  std::unique_ptr<SessionManager> session_manager_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_ENV_H_
