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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_NAMED_SCHEMA_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_NAMED_SCHEMA_BUILDER_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/validators/named_schema_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new named schema
class NamedSchema::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new NamedSchema(NamedSchemaValidator::Validate,
                            NamedSchemaValidator::ValidateUpdate))) {}

  std::unique_ptr<const NamedSchema> build() { return std::move(instance_); }

  const NamedSchema* get() const { return instance_.get(); }

  Builder& set_id(const std::string& id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& add_table(const Table* table) {
    instance_->tables_.push_back(table);
    return *this;
  }

  Builder& add_view(const View* view) {
    instance_->views_.push_back(view);
    return *this;
  }

  Builder& add_index(const Index* index) {
    instance_->indexes_.push_back(index);
    return *this;
  }

  Builder& add_sequence(const Sequence* sequence) {
    instance_->sequences_.push_back(sequence);
    return *this;
  }

 private:
  std::unique_ptr<NamedSchema> instance_;
};

class NamedSchema::Editor {
 public:
  explicit Editor(NamedSchema* instance) : instance_(instance) {}

  const NamedSchema* get() const { return instance_; }

  Editor& add_table(const Table* table) {
    instance_->tables_.push_back(table);
    return *this;
  }

  Editor& add_view(const View* view) {
    instance_->views_.push_back(view);
    return *this;
  }

  Editor& add_index(const Index* index) {
    instance_->indexes_.push_back(index);
    return *this;
  }

  Editor& add_sequence(const Sequence* sequence) {
    instance_->sequences_.push_back(sequence);
    return *this;
  }

 private:
  // Not owned.
  NamedSchema* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_NAMED_SCHEMA_BUILDER_H_
