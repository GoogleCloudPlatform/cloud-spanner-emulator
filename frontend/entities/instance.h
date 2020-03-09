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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_INSTANCE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_INSTANCE_H_

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/strings/string_view.h"
#include "frontend/common/labels.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Instance represents a Cloud Spanner instance in the emulator.
//
// An instance created in the emulator is always in READY state, until deleted.
// Labels are currently not supported for instances in the emulator.
class Instance {
 public:
  Instance(const std::string& name, const std::string config,
           const std::string& display_name, int32_t node_count, Labels labels)
      : name_(name),
        config_(config),
        display_name_(display_name),
        node_count_(node_count),
        labels_(labels) {}

  // Returns the URI for this instance
  const std::string& instance_uri() const { return name_; }

  // Converts this instance object to its proto representation.
  void ToProto(admin::instance::v1::Instance* instance) const;

 private:
  // The name for this instance.
  std::string name_;

  // The instance config used to create this instance.
  std::string config_;

  // The display name for this instance.
  std::string display_name_;

  // The number of nodes in this instance.
  int32_t node_count_;

  // The labels for this instance.
  Labels labels_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_INSTANCE_H_
