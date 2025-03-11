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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_LOCALITY_GROUP_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_LOCALITY_GROUP_BUILDER_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/validators/locality_group_validator.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new locality group.
class LocalityGroup::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new LocalityGroup(LocalityGroupValidator::Validate,
                              LocalityGroupValidator::ValidateUpdate))) {}

  std::unique_ptr<const LocalityGroup> build() { return std::move(instance_); }
  const LocalityGroup* get() const { return instance_.get(); }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_inflash(bool inflash) {
    instance_->inflash_ = inflash;
    return *this;
  }

  Builder& set_ssd_to_hdd_spill_timespans(
      ::google::protobuf::RepeatedPtrField<std::string> ssd_to_hdd_spill_timespans) {
    instance_->ssd_to_hdd_spill_timespans_ = ssd_to_hdd_spill_timespans;
    return *this;
  }

  Builder& set_options(::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

 private:
  std::unique_ptr<LocalityGroup> instance_;
};

class LocalityGroup::Editor {
 public:
  explicit Editor(LocalityGroup* instance) : instance_(instance) {}

  const LocalityGroup* get() const { return instance_; }

  Editor& set_inflash(bool inflash) {
    instance_->inflash_ = inflash;
    return *this;
  }

  Editor& set_ssd_to_hdd_spill_timespans(
      ::google::protobuf::RepeatedPtrField<std::string> ssd_to_hdd_spill_timespans) {
    instance_->ssd_to_hdd_spill_timespans_ = ssd_to_hdd_spill_timespans;

    return *this;
  }

  Editor& increment_use_count() {
    instance_->use_count_++;
    return *this;
  }

  Editor& decrement_use_count() {
    instance_->use_count_--;
    return *this;
  }

  Editor& set_options(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

 private:
  // Not owned.
  LocalityGroup* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_LOCALITY_GROUP_BUILDER_H_
