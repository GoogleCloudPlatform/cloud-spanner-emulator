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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COLLECTIONS_INSTANCE_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COLLECTIONS_INSTANCE_MANAGER_H_

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "frontend/entities/instance.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// InstanceManager manages the set of active instances in the emulator.
class InstanceManager {
 public:
  // Creates a new instance with the given URI.
  zetasql_base::StatusOr<std::shared_ptr<Instance>> CreateInstance(
      const std::string& instance_uri,
      const admin::instance::v1::Instance& instance_proto)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns an instance with the given URI.
  zetasql_base::StatusOr<std::shared_ptr<Instance>> GetInstance(
      const std::string& instance_uri) const ABSL_LOCKS_EXCLUDED(mu_);

  // Deletes an instance with the given URI.
  void DeleteInstance(const std::string& instance_uri) ABSL_LOCKS_EXCLUDED(mu_);

  // Lists all instances associated with the given project URI.
  zetasql_base::StatusOr<std::vector<std::shared_ptr<Instance>>> ListInstances(
      const std::string& project_uri) const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // Mutex to guard state below.
  mutable absl::Mutex mu_;

  // Map from instance URI to instance objects.
  std::map<std::string, std::shared_ptr<Instance>> instances_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COLLECTIONS_INSTANCE_MANAGER_H_
