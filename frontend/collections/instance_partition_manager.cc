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

#include "frontend/collections/instance_partition_manager.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "common/errors.h"
#include "frontend/entities/instance_partition.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace instance_api = ::google::spanner::admin::instance::v1;

absl::StatusOr<std::vector<std::shared_ptr<InstancePartition>>>
InstancePartitionManager::ListInstancePartitions(
    const std::string& instance_uri) const {
  absl::ReaderMutexLock lock(mu_);
  std::vector<std::shared_ptr<InstancePartition>> partitions;
  std::string prefix = absl::StrCat(instance_uri, "/instancePartitions/");
  auto itr = partitions_.lower_bound(prefix);
  while (itr != partitions_.end()) {
    if (!absl::StartsWith(itr->first, prefix)) {
      break;
    }
    partitions.push_back(itr->second);
    ++itr;
  }
  return partitions;
}

absl::StatusOr<std::shared_ptr<InstancePartition>>
InstancePartitionManager::GetInstancePartition(
    const std::string& partition_uri) const {
  absl::ReaderMutexLock lock(mu_);
  auto itr = partitions_.find(partition_uri);
  if (itr == partitions_.end()) {
    return error::InstancePartitionNotFound(partition_uri);
  }
  return itr->second;
}

absl::StatusOr<std::shared_ptr<InstancePartition>>
InstancePartitionManager::CreateInstancePartition(
    const std::string& partition_uri,
    const instance_api::InstancePartition& partition_proto) {
  absl::MutexLock lock(mu_);
  if (partition_proto.node_count() > 0 &&
      partition_proto.processing_units() > 0) {
    return error::InvalidCreateInstancePartitionRequestUnitsNotBoth();
  }
  if (partition_proto.processing_units() > 0 &&
      partition_proto.processing_units() < 1000 &&
      partition_proto.processing_units() % 100 != 0) {
    return error::InvalidCreateInstancePartitionRequestUnitsMultiple();
  }
  if (partition_proto.processing_units() > 1000 &&
      partition_proto.processing_units() % 1000 != 0) {
    return error::InvalidCreateInstancePartitionRequestUnitsMultiple();
  }
  int32_t processing_units;
  if (partition_proto.node_count() > 0) {
    processing_units = partition_proto.node_count() * 1000;
  } else {
    processing_units = partition_proto.processing_units();
  }
  auto inserted = partitions_.insert(
      {partition_uri,
       std::make_shared<InstancePartition>(
           partition_uri, partition_proto.config(),
           partition_proto.display_name(), partition_proto.node_count(),
           processing_units, googlesql_base::Clock::RealClock())});
  if (!inserted.second) {
    return error::InstancePartitionAlreadyExists(partition_uri);
  }
  return inserted.first->second;
}

void InstancePartitionManager::DeleteInstancePartition(
    const std::string& partition_uri) {
  absl::MutexLock lock(mu_);
  partitions_.erase(partition_uri);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
