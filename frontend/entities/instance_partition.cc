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

#include "frontend/entities/instance_partition.h"

#include <cstdint>
#include <string>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "frontend/converters/time.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace instance_api = ::google::spanner::admin::instance::v1;

InstancePartition::InstancePartition(const std::string& name,
                                     const std::string& config,
                                     const std::string& display_name,
                                     int32_t node_count,
                                     int32_t processing_units,
                                     googlesql_base::Clock* clock)
    : name_(name),
      config_(config),
      display_name_(display_name),
      node_count_(node_count),
      processing_units_(processing_units) {
  auto current_time = clock->TimeNow();
  create_time_ = current_time;
  update_time_ = current_time;
}

void InstancePartition::ToProto(
    instance_api::InstancePartition* partition) const {
  partition->Clear();
  partition->set_name(name_);
  partition->set_config(config_);
  partition->set_display_name(display_name_);
  if (node_count_ > 0) {
    partition->set_node_count(node_count_);
  } else {
    partition->set_processing_units(processing_units_);
  }
  partition->set_state(instance_api::InstancePartition::READY);
  if (auto create_time = TimestampToProto(create_time_); create_time.ok()) {
    *partition->mutable_create_time() = *create_time;
  }
  if (auto update_time = TimestampToProto(update_time_); update_time.ok()) {
    *partition->mutable_update_time() = *update_time;
  }
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
