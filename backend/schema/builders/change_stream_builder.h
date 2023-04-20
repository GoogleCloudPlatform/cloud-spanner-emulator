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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHANGE_STREAM_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHANGE_STREAM_BUILDER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/validators/change_stream_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new change stream
class ChangeStream::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new ChangeStream(ChangeStreamValidator::Validate,
                             ChangeStreamValidator::ValidateUpdate))) {}

  std::unique_ptr<const ChangeStream> build() { return std::move(instance_); }

  const ChangeStream* get() const { return instance_.get(); }

  Builder& set_id(const std::string& id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

 private:
  std::unique_ptr<ChangeStream> instance_;
};

class ChangeStream::Editor {
 public:
  explicit Editor(ChangeStream* instance) : instance_(instance) {}

  const ChangeStream* get() const { return instance_; }

 private:
  // Not owned.
  ChangeStream* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHANGE_STREAM_BUILDER_H_
