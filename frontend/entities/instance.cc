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

#include "frontend/entities/instance.h"

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/strings/string_view.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

void Instance::ToProto(admin::instance::v1::Instance* instance) const {
  instance->Clear();
  instance->set_name(name_);
  instance->set_config(config_);
  instance->set_display_name(display_name_);
  instance->set_node_count(node_count_);
  instance->mutable_labels()->insert(labels_.begin(), labels_.end());
  // Instances are always in ready state.
  instance->set_state(admin::instance::v1::Instance::READY);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
