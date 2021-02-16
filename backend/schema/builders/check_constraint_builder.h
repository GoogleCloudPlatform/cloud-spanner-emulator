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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHECK_CONSTRAINT_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHECK_CONSTRAINT_BUILDER_H_

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/validators/check_constraint_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new check constraint.
class CheckConstraint::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique<CheckConstraint>(
            new CheckConstraint(CheckConstraintValidator::Validate,
                                CheckConstraintValidator::ValidateUpdate))) {}

  std::unique_ptr<const CheckConstraint> build() {
    return std::move(instance_);
  }

  const CheckConstraint* get() const { return instance_.get(); }

  Builder& set_constraint_name(const std::string& constraint_name) {
    instance_->constraint_name_ = constraint_name;
    return *this;
  }

  Builder& has_generated_name(bool has_generated_name) {
    instance_->has_generated_name_ = has_generated_name;
    return *this;
  }

  Builder& add_dependent_column(const Column* column) {
    instance_->dependent_columns_.push_back(column);
    return *this;
  }

  Builder& set_expression(const std::string& expression) {
    instance_->expression_ = expression;
    return *this;
  }

  Builder& set_table(const Table* table) {
    instance_->table_ = table;
    return *this;
  }

 private:
  std::unique_ptr<CheckConstraint> instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHECK_CONSTRAINT_BUILDER_H_
