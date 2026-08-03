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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COLLECTIONS_INSTANCE_PARTITION_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COLLECTIONS_INSTANCE_PARTITION_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "frontend/entities/instance_partition.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// InstancePartitionManager manages the set of active instance partitions in the
// emulator.
class InstancePartitionManager {
 public:
  // Creates a new instance partition with the given URI.
  absl::StatusOr<std::shared_ptr<InstancePartition>> CreateInstancePartition(
      const std::string& partition_uri,
      const admin::instance::v1::InstancePartition& partition_proto)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns an instance partition with the given URI.
  absl::StatusOr<std::shared_ptr<InstancePartition>> GetInstancePartition(
      const std::string& partition_uri) const ABSL_LOCKS_EXCLUDED(mu_);

  // Deletes an instance partition with the given URI.
  void DeleteInstancePartition(const std::string& partition_uri)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Lists all instance partitions associated with the given instance URI.
  absl::StatusOr<std::vector<std::shared_ptr<InstancePartition>>>
  ListInstancePartitions(const std::string& instance_uri) const
      ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // Mutex to guard state below.
  mutable absl::Mutex mu_;

  // Map from instance partition URI to instance partition objects.
  absl::btree_map<std::string, std::shared_ptr<InstancePartition>> partitions_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COLLECTIONS_INSTANCE_PARTITION_MANAGER_H_
