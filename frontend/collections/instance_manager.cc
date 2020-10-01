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

#include "frontend/collections/instance_manager.h"

#include <memory>

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/memory/memory.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "common/errors.h"
#include "frontend/common/labels.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace instance_api = ::google::spanner::admin::instance::v1;

zetasql_base::StatusOr<std::vector<std::shared_ptr<Instance>>>
InstanceManager::ListInstances(const std::string& project_uri) const {
  absl::MutexLock lock(&mu_);
  std::vector<std::shared_ptr<Instance>> instances;
  // Find all instance that belongs to the project.
  auto itr = instances_.lower_bound(absl::StrCat(project_uri, "/"));
  while (itr != instances_.end()) {
    if (!absl::StartsWith(itr->first, project_uri)) {
      break;
    }

    instances.push_back(itr->second);
    ++itr;
  }
  return instances;
}

zetasql_base::StatusOr<std::shared_ptr<Instance>> InstanceManager::GetInstance(
    const std::string& instance_uri) const {
  absl::MutexLock lock(&mu_);
  auto itr = instances_.find(instance_uri);
  if (itr == instances_.end()) {
    return error::InstanceNotFound(instance_uri);
  }
  return itr->second;
}

zetasql_base::StatusOr<std::shared_ptr<Instance>> InstanceManager::CreateInstance(
    const std::string& instance_uri,
    const instance_api::Instance& instance_proto) {
  absl::MutexLock lock(&mu_);
  Labels labels(instance_proto.labels().begin(), instance_proto.labels().end());
  auto inserted = instances_.insert(
      {instance_uri,
       std::make_shared<Instance>(instance_uri, instance_proto.config(),
                                  instance_proto.display_name(),
                                  instance_proto.node_count(), labels)});
  if (!inserted.second) {
    return error::InstanceAlreadyExists(instance_uri);
  }
  return inserted.first->second;
}

void InstanceManager::DeleteInstance(const std::string& instance_uri) {
  absl::MutexLock lock(&mu_);
  instances_.erase(instance_uri);
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
