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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_PLACEMENT_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_PLACEMENT_BUILDER_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "backend/schema/catalog/placement.h"
#include "backend/schema/validators/placement_validator.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Placement::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new Placement(PlacementValidator::Validate,
                          PlacementValidator::ValidateUpdate))) {}

  std::unique_ptr<const Placement> build() { return std::move(instance_); }

  const Placement* get() const { return instance_.get(); }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_options(::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

 private:
  std::unique_ptr<Placement> instance_;
};

class Placement::Editor {
 public:
  explicit Editor(Placement* instance) : instance_(instance) {}

  const Placement* get() const { return instance_; }

  Editor& set_options(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

  Editor& set_default_leader(std::optional<std::string> default_leader) {
    instance_->default_leader_ = default_leader;
    return *this;
  }

  Editor& set_instance_partition(
      std::optional<std::string> instance_partition) {
    instance_->instance_partition_ = instance_partition;
    return *this;
  }

 private:
  // Not owned.
  Placement* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_PLACEMENT_BUILDER_H_
