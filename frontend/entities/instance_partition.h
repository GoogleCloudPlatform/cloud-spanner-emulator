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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_INSTANCE_PARTITION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_INSTANCE_PARTITION_H_

#include <cstdint>
#include <string>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/time/time.h"
#include "googlesql/base/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// InstancePartition represents an instance partition in the emulator.
//
// An instance partition created in the emulator is always in READY state,
// until deleted.
class InstancePartition {
 public:
  InstancePartition(const std::string& name, const std::string& config,
                    const std::string& display_name, int32_t node_count,
                    int32_t processing_units, googlesql_base::Clock* clock);

  // Returns the URI for this instance partition.
  const std::string& partition_uri() const { return name_; }

  // Returns the number of nodes in this instance partition.
  int32_t node_count() const { return node_count_; }

  // Returns the number of processing units in this instance partition.
  int32_t processing_units() const { return processing_units_; }

  // Converts this instance partition object to its proto representation.
  void ToProto(admin::instance::v1::InstancePartition* partition) const;

 private:
  // The name (URI) for this instance partition.
  std::string name_;

  // The instance config used by this instance partition.
  std::string config_;

  // The display name for this instance partition.
  std::string display_name_;

  // The number of nodes in this instance partition.
  int32_t node_count_;

  // The number of processing units in this instance partition.
  int32_t processing_units_;

  // Creation time.
  absl::Time create_time_;

  // Update time.
  absl::Time update_time_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_INSTANCE_PARTITION_H_
