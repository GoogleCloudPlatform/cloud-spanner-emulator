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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_FOREIGN_KEY_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_FOREIGN_KEY_BUILDER_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/validators/foreign_key_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new foreign key.
class ForeignKey::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique<ForeignKey>(
            new ForeignKey(ForeignKeyValidator::Validate,
                           ForeignKeyValidator::ValidateUpdate))) {}

  std::unique_ptr<const ForeignKey> build() { return std::move(instance_); }

  const ForeignKey* get() const { return instance_.get(); }

  Builder& set_constraint_name(const std::string& constraint_name) {
    instance_->constraint_name_ = constraint_name;
    return *this;
  }

  Builder& set_generated_name(const std::string& generated_name) {
    instance_->generated_name_ = generated_name;
    return *this;
  }

  Builder& set_referencing_table(const Table* table) {
    instance_->referencing_table_ = table;
    return *this;
  }

  Builder& add_referencing_column(const Column* column) {
    instance_->referencing_columns_.push_back(column);
    return *this;
  }

  Builder& set_referenced_table(const Table* table) {
    instance_->referenced_table_ = table;
    return *this;
  }

  Builder& add_referenced_column(const Column* column) {
    instance_->referenced_columns_.push_back(column);
    return *this;
  }

 private:
  std::unique_ptr<ForeignKey> instance_;
};

// Assign the backing indexes to a foreign key.
class ForeignKey::Editor {
 public:
  explicit Editor(ForeignKey* instance) : instance_(instance) {}

  Editor& set_referencing_index(const Index* index) {
    instance_->referencing_index_ = index;
    return *this;
  }

  Editor& set_referenced_index(const Index* index) {
    instance_->referenced_index_ = index;
    return *this;
  }

 private:
  // Not owned.
  ForeignKey* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_FOREIGN_KEY_BUILDER_H_
