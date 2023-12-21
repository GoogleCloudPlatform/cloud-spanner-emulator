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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_SEQUENCE_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_SEQUENCE_BUILDER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/validators/sequence_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new sequence
class Sequence::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(new Sequence(
            SequenceValidator::Validate, SequenceValidator::ValidateUpdate))) {}

  std::unique_ptr<const Sequence> build() { return std::move(instance_); }

  const Sequence* get() const { return instance_.get(); }

  Builder& set_id(const std::string& id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_sequence_kind(Sequence::SequenceKind sequence_kind) {
    instance_->sequence_kind_ = sequence_kind;
    return *this;
  }

  Builder& set_start_with_counter(int64_t start_with_counter) {
    instance_->start_with_ = start_with_counter;
    instance_->ResetSequenceLastValue();
    return *this;
  }

  Builder& set_skip_range_min(int64_t skip_range_min) {
    instance_->skip_range_min_ = skip_range_min;
    return *this;
  }

  Builder& set_skip_range_max(int64_t skip_range_max) {
    instance_->skip_range_max_ = skip_range_max;
    return *this;
  }

 private:
  std::unique_ptr<Sequence> instance_;
};

class Sequence::Editor {
 public:
  explicit Editor(Sequence* instance) : instance_(instance) {}

  const Sequence* get() const { return instance_; }

  Editor& set_sequence_kind(Sequence::SequenceKind sequence_kind) {
    instance_->sequence_kind_ = sequence_kind;
    return *this;
  }

  Editor& set_start_with_counter(int64_t start_with_counter) {
    instance_->start_with_ = start_with_counter;
    instance_->ResetSequenceLastValue();
    return *this;
  }

  Editor& clear_start_with_counter() {
    instance_->start_with_.reset();
    instance_->ResetSequenceLastValue();
    return *this;
  }

  Editor& set_skip_range_min(int64_t skip_range_min) {
    instance_->skip_range_min_ = skip_range_min;
    return *this;
  }

  Editor& clear_skip_range_min() {
    instance_->skip_range_min_.reset();
    return *this;
  }

  Editor& set_skip_range_max(int64_t skip_range_max) {
    instance_->skip_range_max_ = skip_range_max;
    return *this;
  }

  Editor& clear_skip_range_max() {
    instance_->skip_range_max_.reset();
    return *this;
  }

 private:
  // Not owned.
  Sequence* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_SEQUENCE_BUILDER_H_
